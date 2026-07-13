use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use duckdb::Connection;

use super::{
    accumulate_pair_component_summary, chain_pair_count, chain_pair_from_index,
    execute_progress_batch, format_byte_size, new_chain_matrix_reuse_states, summary_row,
    total_memory_budget_bytes, AnalysisError, GroupSummary, NameTotals, ProgressTracker,
    SparseUnionFind, SummaryRow, SummarySpec, UnionFind, SPARSE_UNION_NODE_BYTES,
};

mod bm25;
mod index;
mod load;
mod parse;

#[cfg(test)]
pub(super) use load::metadata_raw_rows_sql;
pub(super) use parse::MAX_METADATA_BYTES_FOR_DEDUP;

use bm25::*;
use index::*;
use load::*;

pub(crate) fn prepare_metadata_compact_tables(
    conn: &Connection,
    progress: &ProgressTracker,
) -> Result<(), AnalysisError> {
    progress.start_phase("preparing compact metadata sources", 1);
    execute_progress_batch(
        conn,
        metadata_contract_token_rows_sql(),
        progress,
        "filtered singleton token IDs and materialized compact sources",
    )?;
    progress.finish_phase("compact metadata sources ready");
    Ok(())
}

pub(super) fn metadata_memory_budget_bytes(value: &str) -> Result<usize, AnalysisError> {
    total_memory_budget_bytes(value)
}

pub(super) const METADATA_THRESHOLD: f64 = 0.6;
const METADATA_MATCH_MODE: &str = "template_recall_hybrid_verify";
#[cfg(test)]
pub(super) const METADATA_PAIR_LEFT_CHUNK_SIZE: usize = 256;
pub(super) const METADATA_CONTENT_PARALLEL_MIN_RECORDS: usize = 64;
pub(super) const METADATA_CONTENT_SCORE_BATCH_PAIRS: usize = 16 * 1024;
const METADATA_REUSE_CACHE_BUDGET_DIVISOR: usize = 16;
// Debug builds retain substantially larger Rayon/JSON/scoring frames than
// optimized builds. The explicit worker stack prevents data-dependent aborts;
// Linux commits these stack pages on demand rather than eagerly.
const METADATA_ANALYSIS_WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;
pub(super) type MetadataDocKey = String;
pub(super) type MetadataContractIndex = u32;
pub(super) type MetadataDocIndex = u32;
#[cfg(test)]
pub(super) type MetadataDocPair = (MetadataDocIndex, MetadataDocIndex);

#[derive(Debug)]
pub(crate) enum MetadataTemplateDocument {
    Owned(MetadataBm25Document),
    Shared(Arc<MetadataBm25Document>),
}

impl MetadataTemplateDocument {
    fn owned_payload_bytes(&self) -> usize {
        match self {
            Self::Owned(document) => document.memory_bytes(),
            Self::Shared(_) => 0,
        }
    }

    #[cfg(test)]
    fn is_owned(&self) -> bool {
        matches!(self, Self::Owned(_))
    }

    #[cfg(test)]
    fn is_shared(&self) -> bool {
        matches!(self, Self::Shared(_))
    }

    #[cfg(test)]
    fn shares_allocation_with(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Shared(left), Self::Shared(right)) if Arc::ptr_eq(left, right))
    }
}

impl std::ops::Deref for MetadataTemplateDocument {
    type Target = MetadataBm25Document;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(document) => document,
            Self::Shared(document) => document,
        }
    }
}

impl From<MetadataBm25Document> for MetadataTemplateDocument {
    fn from(document: MetadataBm25Document) -> Self {
        Self::Owned(document)
    }
}

impl From<Arc<MetadataBm25Document>> for MetadataTemplateDocument {
    fn from(document: Arc<MetadataBm25Document>) -> Self {
        Self::Shared(document)
    }
}

#[derive(Clone, Debug)]
pub(super) struct MetadataContract {
    pub(super) chain_index: usize,
    pub(super) nft_count: i64,
    pub(super) content_doc: Option<Arc<MetadataBm25Document>>,
    pub(super) template_doc_index: MetadataDocIndex,
    pub(super) uses_declared_metadata_source: bool,
}

#[derive(Debug)]
pub(super) struct SourceMetadataDocEntry {
    pub(super) doc: MetadataTemplateDocument,
    pub(super) contracts: Vec<MetadataContractIndex>,
}

#[derive(Debug)]
pub(super) struct MetadataData {
    pub(super) contracts: Vec<MetadataContract>,
    pub(super) contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    pub(super) compact_contract_indexes_by_source: Vec<Option<MetadataContractIndex>>,
    pub(super) metadata_index: InternedMetadataIndex,
    pub(super) reused_documents: ReusedMetadataDocuments,
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct MetadataTemplateMatches {
    compatible_docs: CompactMetadataPostings,
}

pub(super) struct MetadataDataBuilder {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    source_contract_indexes: Vec<u32>,
    docs: Vec<SourceMetadataDocEntry>,
    doc_index_by_key: HashMap<MetadataDocKey, usize>,
    document_payload_bytes: usize,
    doc_contract_bytes: usize,
    doc_key_bytes: usize,
    content_doc_bytes: usize,
    seen_content_docs: HashSet<usize>,
    template_unique_terms: usize,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct MetadataAlgorithmMetrics {
    eligible_rows: u64,
    selected_sources: u64,
    reused_raw_json_cache_entries: u64,
    singleton_tokens_removed: u64,
    retained_shared_tokens: u64,
    template_documents: u64,
    content_documents: u64,
    template_candidate_pairs: u64,
    template_scored_pairs: u64,
    template_matched_pairs: u64,
    content_atoms: u64,
    content_candidate_pairs: u64,
    content_scored_pairs: u64,
    mmap_bytes: u64,
    dsu_bytes: u64,
}

pub(crate) struct MetadataAnalysisResult {
    pub(crate) rows: Vec<SummaryRow>,
    pub(crate) metrics: MetadataAlgorithmMetrics,
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

#[cfg(test)]
impl MetadataTemplateMatches {
    pub(super) fn from_pairs(doc_count: usize, mut pairs: Vec<MetadataDocPair>) -> Self {
        for (left, right) in &mut pairs {
            if *left > *right {
                std::mem::swap(left, right);
            }
        }
        pairs.sort_unstable();
        pairs.dedup();
        Self {
            compatible_docs: CompactMetadataPostings::from_symmetric_pairs(doc_count, &pairs),
        }
    }

    pub(super) fn matches(&self, left: usize, right: usize) -> bool {
        left == right
            || self
                .compatible_docs(metadata_doc_index_from_usize(left))
                .binary_search(&metadata_doc_index_from_usize(right))
                .is_ok()
    }

    pub(super) fn compatible_docs(&self, doc: MetadataDocIndex) -> &[MetadataDocIndex] {
        let doc = metadata_doc_index_to_usize(doc);
        if doc >= self.compatible_docs.len() {
            &[]
        } else {
            self.compatible_docs.posting(doc)
        }
    }

    pub(super) fn owned_memory_bytes(&self) -> usize {
        self.compatible_docs.owned_memory_bytes()
    }

    pub(super) fn remap_if_over_budget(
        &mut self,
        directory: &Path,
        maximum_owned_bytes: usize,
    ) -> std::io::Result<bool> {
        if self.owned_memory_bytes() <= maximum_owned_bytes {
            return Ok(false);
        }
        self.remap(directory)?;
        Ok(true)
    }

    pub(super) fn remap(&mut self, directory: &Path) -> std::io::Result<()> {
        let compatible_docs = std::mem::replace(
            &mut self.compatible_docs,
            CompactMetadataPostings::from_nested(Vec::new()),
        );
        self.compatible_docs = compatible_docs.persist_and_remap_named(
            directory,
            "template_match_offsets.bin",
            "template_matches.bin",
        )?;
        Ok(())
    }
}

#[cfg(test)]
impl Default for MetadataTemplateMatches {
    fn default() -> Self {
        Self {
            compatible_docs: CompactMetadataPostings::from_nested(Vec::new()),
        }
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
            document_payload_bytes: 0,
            doc_contract_bytes: 0,
            doc_key_bytes: 0,
            content_doc_bytes: 0,
            seen_content_docs: HashSet::new(),
            template_unique_terms: 0,
        }
    }

    pub(super) fn merge_indexed_rows(
        &mut self,
        indexed_rows: Vec<(u32, IndexedMetadataRow)>,
        uses_declared_metadata_source: bool,
    ) {
        for (source_contract_index, row) in indexed_rows {
            self.merge_source_indexed_row(
                source_contract_index,
                row,
                uses_declared_metadata_source,
            );
        }
    }

    pub(super) fn memory_bytes(&self) -> usize {
        let contracts = self
            .contracts
            .capacity()
            .saturating_mul(std::mem::size_of::<MetadataContract>());
        let chains = self
            .contracts_by_chain
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<MetadataContractIndex>>())
            .saturating_add(
                self.contracts_by_chain
                    .iter()
                    .map(|contracts| {
                        contracts
                            .capacity()
                            .saturating_mul(std::mem::size_of::<MetadataContractIndex>())
                    })
                    .fold(0usize, usize::saturating_add),
            );
        let sources = self
            .source_contract_indexes
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>());
        let docs = self
            .docs
            .capacity()
            .saturating_mul(std::mem::size_of::<SourceMetadataDocEntry>())
            .saturating_add(self.document_payload_bytes)
            .saturating_add(self.doc_contract_bytes)
            .saturating_add(self.content_doc_bytes);
        let lookup = hash_table_allocation_bytes(
            self.doc_index_by_key.capacity(),
            std::mem::size_of::<(MetadataDocKey, usize)>(),
        )
        .saturating_add(self.doc_key_bytes)
        .saturating_add(hash_table_allocation_bytes(
            self.seen_content_docs.capacity(),
            std::mem::size_of::<usize>(),
        ));
        contracts
            .saturating_add(chains)
            .saturating_add(sources)
            .saturating_add(docs)
            .saturating_add(lookup)
    }

    fn lookup_memory_bytes(&self) -> usize {
        hash_table_allocation_bytes(
            self.doc_index_by_key.capacity(),
            std::mem::size_of::<(MetadataDocKey, usize)>(),
        )
        .saturating_add(self.doc_key_bytes)
        .saturating_add(hash_table_allocation_bytes(
            self.seen_content_docs.capacity(),
            std::mem::size_of::<usize>(),
        ))
    }

    /// Conservative peak for converting the loaded string documents into the
    /// compact BM25 arrays. The lookup maps are dropped before this conversion;
    /// the estimate covers the overlapping lexical dictionary, source docs,
    /// prepared queries/docs and final flat arrays, plus 25% allocator and
    /// HashMap slack. No global template-pair graph is materialized; compact
    /// per-document safe-prefix tokens are included in the flat arrays.
    pub(super) fn estimated_finish_peak_memory_bytes(&self) -> usize {
        let document_count = self.docs.len();
        let unique_terms = self.template_unique_terms;

        let lexical_dictionary = unique_terms.saturating_mul(
            std::mem::size_of::<&str>()
                .saturating_add(std::mem::size_of::<(&str, usize)>())
                .saturating_add(1),
        );
        let source_documents = document_count
            .saturating_mul(std::mem::size_of::<InternedMetadataSourceDoc>())
            .saturating_add(unique_terms.saturating_mul(std::mem::size_of::<(u32, u32)>()));
        let prepared_and_queries = document_count
            .saturating_mul(
                std::mem::size_of::<usize>()
                    .saturating_add(std::mem::size_of::<PreparedInternedMetadataDoc>())
                    .saturating_add(std::mem::size_of::<PreparedInternedMetadataQuery>()),
            )
            .saturating_add(
                unique_terms.saturating_mul(
                    4usize
                        .saturating_mul(std::mem::size_of::<usize>())
                        .saturating_add(2 * std::mem::size_of::<f64>()),
                ),
            );
        let compact_scoring = document_count
            .saturating_add(1)
            .saturating_mul(4 * std::mem::size_of::<u64>())
            .saturating_add(document_count.saturating_mul(std::mem::size_of::<f64>()))
            .saturating_add(
                unique_terms.saturating_mul(
                    3usize
                        .saturating_mul(std::mem::size_of::<u32>())
                        .saturating_add(std::mem::size_of::<f64>()),
                ),
            );
        let corpus = unique_terms
            .saturating_mul(2 * std::mem::size_of::<usize>())
            .saturating_add(std::mem::size_of::<InternedMetadataCorpus>());
        let conversion_working_bytes = lexical_dictionary
            .saturating_add(source_documents)
            .saturating_add(prepared_and_queries)
            .saturating_add(compact_scoring)
            .saturating_add(corpus);
        let conversion_with_slack =
            conversion_working_bytes.saturating_add(conversion_working_bytes.saturating_div(4));
        let post_lookup_builder = self
            .memory_bytes()
            .saturating_sub(self.lookup_memory_bytes());
        self.memory_bytes()
            .max(post_lookup_builder.saturating_add(conversion_with_slack))
    }

    pub(super) fn ensure_within_memory_budget(
        &self,
        maximum_bytes: usize,
    ) -> Result<(), AnalysisError> {
        let resident_bytes = self.estimated_finish_peak_memory_bytes();
        if resident_bytes > maximum_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "projected metadata index build peak is about {}, exceeding analysis budget {}",
                format_byte_size(resident_bytes),
                format_byte_size(maximum_bytes)
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    fn merge_indexed_row(&mut self, row: IndexedMetadataRow) {
        let source_contract_index = u32::try_from(self.source_contract_indexes.len())
            .expect("metadata source contract index exceeds u32 indexes");
        self.merge_source_indexed_row(source_contract_index, row, true);
    }

    fn merge_source_indexed_row(
        &mut self,
        source_contract_index: u32,
        row: IndexedMetadataRow,
        uses_declared_metadata_source: bool,
    ) {
        if let Some(content_doc) = row.content_doc.as_ref() {
            let pointer = Arc::as_ptr(content_doc) as usize;
            if self.seen_content_docs.insert(pointer) && Arc::strong_count(content_doc) == 1 {
                self.content_doc_bytes = self
                    .content_doc_bytes
                    .saturating_add(metadata_content_arc_memory_bytes(content_doc));
            }
        }
        let doc_index = match self.doc_index_by_key.get(&row.doc_key).copied() {
            Some(index) => index,
            None => {
                let index = self.docs.len();
                self.template_unique_terms = self
                    .template_unique_terms
                    .saturating_add(row.doc.unique_len());
                self.document_payload_bytes = self
                    .document_payload_bytes
                    .saturating_add(row.doc.owned_payload_bytes());
                self.doc_key_bytes = self.doc_key_bytes.saturating_add(row.doc_key.capacity());
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
            content_doc: row.content_doc,
            template_doc_index: compact_doc_index,
            uses_declared_metadata_source,
        });
        self.contracts_by_chain[row.chain_index]
            .push(metadata_contract_index_from_usize(contract_index));
        self.source_contract_indexes.push(source_contract_index);
        let compact_contract_index = metadata_contract_index_from_usize(contract_index);
        let previous_contract_capacity = self.docs[doc_index].contracts.capacity();
        self.docs[doc_index].contracts.push(compact_contract_index);
        self.doc_contract_bytes = self.doc_contract_bytes.saturating_add(
            self.docs[doc_index]
                .contracts
                .capacity()
                .saturating_sub(previous_contract_capacity)
                .saturating_mul(std::mem::size_of::<MetadataContractIndex>()),
        );
    }

    #[cfg(test)]
    pub(super) fn finish(self) -> MetadataData {
        self.finish_with_reused_documents(ReusedMetadataDocuments::new())
    }

    pub(super) fn finish_with_reused_documents(
        self,
        reused_documents: ReusedMetadataDocuments,
    ) -> MetadataData {
        let Self {
            contracts,
            contracts_by_chain,
            source_contract_indexes,
            docs,
            doc_index_by_key,
            seen_content_docs,
            ..
        } = self;
        // These two hash tables are load-only state. Releasing them before the
        // BM25 conversion removes a full copy of every document key from the
        // actual peak.
        drop(doc_index_by_key);
        drop(seen_content_docs);
        let mut compact_contract_indexes_by_source = source_contract_indexes
            .iter()
            .copied()
            .max()
            .map_or_else(Vec::new, |max_source_index| {
                vec![None; max_source_index as usize + 1]
            });
        for (compact_contract_index, source_contract_index) in
            source_contract_indexes.into_iter().enumerate()
        {
            compact_contract_indexes_by_source[source_contract_index as usize] =
                Some(metadata_contract_index_from_usize(compact_contract_index));
        }
        let metadata_index = InternedMetadataIndex::from_source_doc_entries(docs);
        MetadataData {
            contracts,
            contracts_by_chain,
            compact_contract_indexes_by_source,
            metadata_index,
            reused_documents,
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

fn release_metadata_scoring_state(data: &mut MetadataData) {
    data.metadata_index = InternedMetadataIndex::from_source_doc_entries(Vec::new());
    data.compact_contract_indexes_by_source = Vec::new();
    data.reused_documents = ReusedMetadataDocuments::new();
    for contract in &mut data.contracts {
        contract.content_doc = None;
    }
}

pub(super) fn run_metadata_analysis(
    conn: &Connection,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    threads: usize,
    analysis_memory_limit: &str,
    artifact_directory: Option<&Path>,
    progress: &ProgressTracker,
) -> Result<MetadataAnalysisResult, AnalysisError> {
    progress.start_phase("analyzing metadata duplicates", 3);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .thread_name(|index| format!("metadata-{index}"))
        .stack_size(METADATA_ANALYSIS_WORKER_STACK_BYTES)
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
    let eligible_rows = scalar_u64(conn, "SELECT count(*)::UBIGINT FROM metadata_rows")?;
    let selected_sources = scalar_u64(
        conn,
        "SELECT count(*)::UBIGINT
         FROM analysis_contracts
         WHERE metadata_source_file IS NOT NULL",
    )?;
    let (singleton_tokens_removed, retained_shared_tokens) = conn.query_row(
        "SELECT singleton_token_count, retained_shared_token_count
         FROM metadata_token_stats",
        [],
        |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
    )?;
    let analysis_memory_bytes = metadata_memory_budget_bytes(analysis_memory_limit)?;
    let retained_contract_token_rows = scalar_u64(
        conn,
        "SELECT count(*)::UBIGINT FROM metadata_contract_token_rows",
    )?;
    let selected_source_count = usize::try_from(selected_sources).unwrap_or(usize::MAX);
    let retained_contract_token_count =
        usize::try_from(retained_contract_token_rows).unwrap_or(usize::MAX);
    let runtime_reserve_bytes = metadata_runtime_reserve_bytes(
        selected_source_count,
        retained_contract_token_count,
        chains.len(),
    );
    if runtime_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata union/token/summary state needs about {}, exceeding analysis budget {}",
            format_byte_size(runtime_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let build_overlap_reserve_bytes = metadata_build_overlap_reserve_bytes(selected_source_count);
    let load_transient_reserve_bytes =
        metadata_load_transient_reserve_bytes(analysis_memory_bytes, chains)?;
    let concurrent_load_reserve_bytes =
        build_overlap_reserve_bytes.saturating_add(load_transient_reserve_bytes);
    if concurrent_load_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata mapping/load transient state needs about {}, exceeding analysis budget {}",
            format_byte_size(concurrent_load_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let cache_budget_bytes = analysis_memory_bytes
        .saturating_div(METADATA_REUSE_CACHE_BUDGET_DIVISOR)
        .min(analysis_memory_bytes.saturating_sub(runtime_reserve_bytes))
        .min(analysis_memory_bytes.saturating_sub(concurrent_load_reserve_bytes));
    let reused_documents = load_reused_metadata_documents(conn, &pool, Some(cache_budget_bytes))?;
    let reused_cache_bytes = reused_metadata_documents_memory_bytes(&reused_documents);
    let reused_raw_json_cache_entries = reused_documents.len() as u64;
    let builder_peak_budget_bytes = metadata_builder_peak_budget_bytes(
        analysis_memory_bytes,
        build_overlap_reserve_bytes,
        reused_cache_bytes,
        load_transient_reserve_bytes,
    )?;
    let mut data = load_metadata_data(
        conn,
        chains,
        &pool,
        reused_documents,
        MetadataLoadBudgets::new(builder_peak_budget_bytes, load_transient_reserve_bytes),
    )?;
    let contract_token_reserve_bytes =
        metadata_contract_token_reserve_bytes(data.contracts.len(), retained_contract_token_count);
    let pre_token_resident_budget = metadata_pre_token_resident_budget_bytes(
        analysis_memory_bytes,
        contract_token_reserve_bytes,
    )?;
    let mut pre_token_resident_bytes = metadata_resident_memory_bytes(&data, None, chains.len());
    pre_token_resident_bytes = remap_metadata_index_for_resident_budget(
        &mut data,
        pre_token_resident_bytes,
        pre_token_resident_budget,
        artifact_directory,
    )?;
    if pre_token_resident_bytes > pre_token_resident_budget {
        return Err(AnalysisError::InvalidData(format!(
            "metadata resident state needs about {} before loading contract tokens, leaving less than the required {} token reserve inside analysis budget {}",
            format_byte_size(pre_token_resident_bytes),
            format_byte_size(contract_token_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let contract_tokens = load_metadata_contract_tokens(conn, &data, &pool)?;
    let template_document_count = data.metadata_index.doc_count() as u64;
    let content_document_count = data
        .contracts
        .iter()
        .filter(|contract| contract.content_doc.is_some())
        .count() as u64;
    let mut resident_bytes =
        metadata_resident_memory_bytes(&data, Some(&contract_tokens), chains.len());
    resident_bytes = remap_metadata_index_for_resident_budget(
        &mut data,
        resident_bytes,
        analysis_memory_bytes,
        artifact_directory,
    )?;
    if resident_bytes > analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata resident state needs about {}, exceeding analysis budget {}",
            format_byte_size(resident_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let mapped_index_bytes = u64::try_from(data.metadata_index.mapped_bytes()).unwrap_or(u64::MAX);
    progress.step(format!(
        "loaded {} metadata documents for {} contracts",
        data.metadata_index.doc_count(),
        data.contracts.len()
    ));
    let mut rows = Vec::new();
    if data.contracts.len() < 2 || data.metadata_index.is_empty() {
        push_empty_metadata_rows(&mut rows, chains, totals);
        progress.step("metadata scoring skipped");
        progress.step("metadata rows summarized");
        progress.finish_phase("metadata analysis complete");
        return Ok(MetadataAnalysisResult {
            rows,
            metrics: MetadataAlgorithmMetrics {
                eligible_rows,
                selected_sources,
                reused_raw_json_cache_entries,
                singleton_tokens_removed,
                retained_shared_tokens,
                template_documents: template_document_count,
                content_documents: content_document_count,
                template_candidate_pairs: 0,
                template_scored_pairs: 0,
                template_matched_pairs: 0,
                content_atoms: 0,
                content_candidate_pairs: 0,
                content_scored_pairs: 0,
                mmap_bytes: mapped_index_bytes,
                dsu_bytes: 0,
            },
        });
    }

    let mut state = MetadataUnionState {
        intra: UnionFind::new(data.contracts.len()),
        cross: (chains.len() > 1).then(SparseUnionFind::default),
        chain_matrix: (chains.len() > 1)
            .then(|| new_chain_matrix_reuse_states(chain_pair_count(chains.len()))),
    };
    let maximum_shared_working_bytes = analysis_memory_bytes - resident_bytes;
    let mut content_stats = MetadataContentUnionStats::default();
    {
        let content_context = MetadataContentUnionContext {
            data: &data,
            template_compatibility: MetadataTemplateCompatibility::Scored(
                &data.metadata_index.scoring,
            ),
            contract_tokens: &contract_tokens,
            chain_count: chains.len(),
            pool: &pool,
        };
        let shared_stats = union_metadata_token_content_matches(
            conn,
            &content_context,
            &mut state,
            maximum_shared_working_bytes,
        )?;
        content_stats.accumulate(shared_stats);
    }
    drop(std::mem::take(&mut data.reused_documents));
    let fallback_resident_bytes =
        metadata_resident_memory_bytes(&data, Some(&contract_tokens), chains.len());
    if fallback_resident_bytes > analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata resident state needs about {} after releasing the reuse cache, exceeding analysis budget {}",
            format_byte_size(fallback_resident_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    let maximum_fallback_working_bytes = analysis_memory_bytes - fallback_resident_bytes;
    {
        let content_context = MetadataContentUnionContext {
            data: &data,
            template_compatibility: MetadataTemplateCompatibility::Scored(
                &data.metadata_index.scoring,
            ),
            contract_tokens: &contract_tokens,
            chain_count: chains.len(),
            pool: &pool,
        };
        content_stats.accumulate(union_metadata_representative_content_fallback(
            &content_context,
            &mut state,
            maximum_fallback_working_bytes,
        )?);
    }
    progress.step("metadata documents scored");
    drop(contract_tokens);
    drop(pool);
    release_metadata_scoring_state(&mut data);
    let summary_peak_bytes = metadata_summary_peak_memory_bytes(&data, &state);
    if summary_peak_bytes > analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata summary peak needs about {}, exceeding analysis budget {}",
            format_byte_size(summary_peak_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    push_metadata_summary_rows(&mut rows, &data, chains, totals, &mut state);
    progress.step("metadata rows summarized");
    progress.finish_phase("metadata analysis complete");
    let dsu_bytes = metadata_union_state_bytes(&state);
    Ok(MetadataAnalysisResult {
        rows,
        metrics: MetadataAlgorithmMetrics {
            eligible_rows,
            selected_sources,
            reused_raw_json_cache_entries,
            singleton_tokens_removed,
            retained_shared_tokens,
            template_documents: template_document_count,
            content_documents: content_document_count,
            template_candidate_pairs: content_stats.template_candidate_pairs,
            template_scored_pairs: content_stats.template_scored_pairs,
            template_matched_pairs: content_stats.template_matched_pairs,
            content_atoms: content_stats.atom_count as u64,
            content_candidate_pairs: content_stats.candidate_pairs,
            content_scored_pairs: content_stats.scored_pairs,
            mmap_bytes: mapped_index_bytes,
            dsu_bytes,
        },
    })
}

fn scalar_u64(conn: &Connection, sql: &str) -> Result<u64, AnalysisError> {
    Ok(conn.query_row(sql, [], |row| row.get(0))?)
}

fn metadata_union_state_bytes(state: &MetadataUnionState) -> u64 {
    let dense = state
        .intra
        .parent
        .capacity()
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_add(state.intra.rank.capacity());
    let sparse = state
        .cross
        .iter()
        .chain(state.chain_matrix.iter().flatten())
        .map(|union| {
            union
                .atoms
                .capacity()
                .saturating_mul(std::mem::size_of::<usize>())
                .saturating_add(
                    union
                        .parent
                        .capacity()
                        .saturating_mul(std::mem::size_of::<usize>()),
                )
                .saturating_add(union.rank.capacity())
                .saturating_add(hash_table_allocation_bytes(
                    union.index_by_atom.capacity(),
                    std::mem::size_of::<(usize, usize)>(),
                ))
        })
        .fold(0usize, usize::saturating_add);
    dense.saturating_add(sparse) as u64
}

fn metadata_summary_peak_memory_bytes(data: &MetadataData, state: &MetadataUnionState) -> usize {
    let contracts = data
        .contracts
        .capacity()
        .saturating_mul(std::mem::size_of::<MetadataContract>());
    let chains = data
        .contracts_by_chain
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<MetadataContractIndex>>())
        .saturating_add(
            data.contracts_by_chain
                .iter()
                .map(|contracts| {
                    contracts
                        .capacity()
                        .saturating_mul(std::mem::size_of::<MetadataContractIndex>())
                })
                .fold(0usize, usize::saturating_add),
        );
    let dense_scratch = data.contracts.len().saturating_mul(
        std::mem::size_of::<MetadataComponentAccumulator>()
            .saturating_add(std::mem::size_of::<usize>()),
    );
    let maximum_sparse_atoms = state
        .cross
        .iter()
        .chain(state.chain_matrix.iter().flatten())
        .map(SparseUnionFind::atom_count)
        .max()
        .unwrap_or(0);
    let sparse_scratch = hash_table_allocation_for_len_upper(
        maximum_sparse_atoms,
        std::mem::size_of::<(usize, MetadataComponentAccumulator)>()
            .max(std::mem::size_of::<(usize, MetadataPairComponentAccumulator)>()),
    );
    let union_state_bytes =
        usize::try_from(metadata_union_state_bytes(state)).unwrap_or(usize::MAX);
    contracts
        .saturating_add(chains)
        .saturating_add(union_state_bytes)
        .saturating_add(dense_scratch.max(sparse_scratch))
}

fn metadata_runtime_reserve_bytes(
    contract_count: usize,
    retained_contract_token_rows: usize,
    chain_count: usize,
) -> usize {
    let contract_tokens =
        metadata_contract_token_resident_bytes(contract_count, retained_contract_token_rows);
    let source_mapping =
        contract_count.saturating_mul(std::mem::size_of::<Option<MetadataContractIndex>>());
    let dense_dsu = contract_count
        .saturating_mul(std::mem::size_of::<usize>().saturating_add(std::mem::size_of::<u8>()));
    let sparse_state_count = metadata_sparse_membership_factor(chain_count);
    let sparse_dsu = contract_count
        .saturating_mul(SPARSE_UNION_NODE_BYTES)
        .saturating_mul(sparse_state_count);
    let reserve = contract_tokens
        .saturating_add(source_mapping)
        .saturating_add(dense_dsu)
        .saturating_add(sparse_dsu);
    reserve.saturating_add(reserve.saturating_div(8))
}

fn metadata_contract_token_resident_bytes(
    contract_count: usize,
    retained_contract_token_rows: usize,
) -> usize {
    contract_count
        .saturating_add(1)
        .saturating_mul(std::mem::size_of::<u64>())
        .saturating_add(retained_contract_token_rows.saturating_mul(std::mem::size_of::<u32>()))
}

fn metadata_contract_token_reserve_bytes(
    contract_count: usize,
    retained_contract_token_rows: usize,
) -> usize {
    let bytes =
        metadata_contract_token_resident_bytes(contract_count, retained_contract_token_rows)
            .saturating_add(contract_count.saturating_mul(std::mem::size_of::<u32>()));
    bytes.saturating_add(bytes.saturating_div(8))
}

fn metadata_pre_token_resident_budget_bytes(
    analysis_memory_bytes: usize,
    contract_token_reserve_bytes: usize,
) -> Result<usize, AnalysisError> {
    if contract_token_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata contract-token load needs about {}, exceeding analysis budget {}",
            format_byte_size(contract_token_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    Ok(analysis_memory_bytes - contract_token_reserve_bytes)
}

fn metadata_sparse_membership_factor(chain_count: usize) -> usize {
    if chain_count > 1 {
        // Every contract can appear once in the global cross-chain DSU and
        // once in each of the (k - 1) pair matrices involving its own chain.
        chain_count
    } else {
        0
    }
}

fn metadata_build_overlap_reserve_bytes(contract_count: usize) -> usize {
    let mapping =
        contract_count.saturating_mul(std::mem::size_of::<Option<MetadataContractIndex>>());
    mapping.saturating_add(mapping.saturating_div(8))
}

fn metadata_builder_peak_budget_bytes(
    analysis_memory_bytes: usize,
    build_overlap_reserve_bytes: usize,
    reused_cache_bytes: usize,
    load_transient_reserve_bytes: usize,
) -> Result<usize, AnalysisError> {
    let concurrent_reserve_bytes = build_overlap_reserve_bytes
        .saturating_add(reused_cache_bytes)
        .saturating_add(load_transient_reserve_bytes);
    if concurrent_reserve_bytes >= analysis_memory_bytes {
        return Err(AnalysisError::InvalidData(format!(
            "metadata cache/mapping/load transient state needs about {}, exceeding analysis budget {}",
            format_byte_size(concurrent_reserve_bytes),
            format_byte_size(analysis_memory_bytes)
        )));
    }
    Ok(analysis_memory_bytes - concurrent_reserve_bytes)
}

fn remap_metadata_index_for_resident_budget(
    data: &mut MetadataData,
    resident_bytes: usize,
    maximum_resident_bytes: usize,
    artifact_directory: Option<&Path>,
) -> Result<usize, AnalysisError> {
    if resident_bytes <= maximum_resident_bytes {
        return Ok(resident_bytes);
    }
    let Some(directory) = artifact_directory else {
        return Ok(resident_bytes);
    };
    let logical_index_bytes = data.metadata_index.logical_memory_bytes();
    let non_index_bytes = resident_bytes.saturating_sub(logical_index_bytes);
    let maximum_owned_index_bytes = maximum_resident_bytes.saturating_sub(non_index_bytes);
    data.metadata_index
        .remap_if_over_budget(directory, maximum_owned_index_bytes)?;
    Ok(non_index_bytes.saturating_add(data.metadata_index.logical_memory_bytes()))
}

fn metadata_resident_memory_bytes(
    data: &MetadataData,
    contract_tokens: Option<&CompactContractTokens>,
    chain_count: usize,
) -> usize {
    let content_doc_bytes = data
        .contracts
        .iter()
        .filter_map(|contract| contract.content_doc.as_ref())
        .chain(
            data.reused_documents
                .values()
                .filter_map(|document| document.content.as_ref()),
        )
        .map(|document| {
            metadata_content_arc_memory_bytes(document).div_ceil(Arc::strong_count(document).max(1))
        })
        .fold(0usize, usize::saturating_add);
    let contract_bytes = data
        .contracts
        .capacity()
        .saturating_mul(std::mem::size_of::<MetadataContract>())
        .saturating_add(content_doc_bytes);
    let chain_bytes = data
        .contracts_by_chain
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<MetadataContractIndex>>())
        .saturating_add(
            data.contracts_by_chain
                .iter()
                .map(|contracts| {
                    contracts
                        .capacity()
                        .saturating_mul(std::mem::size_of::<MetadataContractIndex>())
                })
                .fold(0usize, usize::saturating_add),
        );
    let mapping_bytes = data
        .compact_contract_indexes_by_source
        .capacity()
        .saturating_mul(std::mem::size_of::<Option<MetadataContractIndex>>());
    let contract_token_bytes = contract_tokens.map_or(0, |tokens| {
        debug_assert_eq!(tokens.len(), data.contracts.len());
        debug_assert_eq!(tokens.is_empty(), data.contracts.is_empty());
        tokens.memory_bytes()
    });
    let reused_bytes = reused_metadata_documents_non_content_memory_bytes(&data.reused_documents);
    let contract_count = data.contracts.len();
    let dense_dsu_bytes = contract_count
        .saturating_mul(std::mem::size_of::<usize>().saturating_add(std::mem::size_of::<u8>()));
    let sparse_state_count = metadata_sparse_membership_factor(chain_count);
    let sparse_dsu_bytes = contract_count
        .saturating_mul(SPARSE_UNION_NODE_BYTES)
        .saturating_mul(sparse_state_count);
    contract_bytes
        .saturating_add(chain_bytes)
        .saturating_add(mapping_bytes)
        .saturating_add(contract_token_bytes)
        .saturating_add(data.metadata_index.logical_memory_bytes())
        .saturating_add(reused_bytes)
        .saturating_add(dense_dsu_bytes)
        .saturating_add(sparse_dsu_bytes)
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
        rows.push(metadata_summary_row("intra_chain", chain, "", total, intra));
    }
    drop(dense_scratch);

    for (chain_index, chain) in chains.iter().enumerate() {
        let total = totals.get(chain).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        if let Some(cross) = &mut state.cross {
            let cross_summary =
                summarize_metadata_sparse_components_for_primary(data, cross, chain_index);
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
        let (left_summary, right_summary) = summarize_metadata_sparse_components_for_chain_pair(
            data,
            union_find,
            left_chain,
            right_chain,
        );
        push_metadata_chain_matrix_row(rows, chains, totals, left_chain, right_chain, left_summary);
        push_metadata_chain_matrix_row(
            rows,
            chains,
            totals,
            right_chain,
            left_chain,
            right_summary,
        );
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
