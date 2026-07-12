use std::collections::{HashMap, HashSet};
use std::ops::Index;
use std::sync::Arc;

use duckdb::Connection;
use rayon::prelude::*;

use super::super::{arrow_i64_column, arrow_string_column, AnalysisError};
use super::bm25::MetadataBm25Document;
use super::parse::{
    metadata_documents_from_json, metadata_is_dedup_eligible, MAX_METADATA_BYTES_FOR_DEDUP,
};
use super::{metadata_contract_index_to_usize, MetadataData, MetadataDataBuilder, MetadataDocKey};

pub(super) const METADATA_LOAD_CHUNK_ROWS: usize = 16 * 1024;
pub(super) const METADATA_FALLBACK_SOURCE_TABLE: &str = "__metadata_fallback_source_indexes";
const METADATA_LOAD_TRANSIENT_BUDGET_DIVISOR: usize = 8;
const METADATA_LOAD_TRANSIENT_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;
const METADATA_PARSE_EXPANSION_MULTIPLIER: usize = 96;
const METADATA_PARSE_ALLOCATOR_SLACK_DIVISOR: usize = 4;

pub(super) fn metadata_uncached_parse_transient_bytes(
    raw_payload_bytes: usize,
    fixed_bytes: usize,
) -> usize {
    let estimated = fixed_bytes
        .saturating_add(raw_payload_bytes)
        .saturating_add(raw_payload_bytes.saturating_mul(METADATA_PARSE_EXPANSION_MULTIPLIER));
    estimated.saturating_add(estimated.saturating_div(METADATA_PARSE_ALLOCATOR_SLACK_DIVISOR))
}

fn metadata_cached_clone_transient_bytes(
    raw_payload_bytes: usize,
    fixed_bytes: usize,
    cached_clone_bytes: usize,
) -> usize {
    let estimated = fixed_bytes
        .saturating_add(raw_payload_bytes)
        .saturating_add(cached_clone_bytes);
    estimated.saturating_add(estimated.saturating_div(METADATA_PARSE_ALLOCATOR_SLACK_DIVISOR))
}

pub(super) struct RawMetadataRow {
    pub(super) chain: String,
    pub(super) metadata_json: String,
    pub(super) nft_count: i64,
}

pub(crate) struct IndexedMetadataRow {
    pub(super) chain_index: usize,
    pub(super) nft_count: i64,
    pub(super) content_doc: Option<Arc<MetadataBm25Document>>,
    pub(super) doc: MetadataBm25Document,
    pub(super) doc_key: MetadataDocKey,
}

#[derive(Clone, Debug)]
pub(crate) struct ReusedMetadataDocument {
    pub(super) prefilter: Option<MetadataBm25Document>,
    pub(super) content: Option<Arc<MetadataBm25Document>>,
    pub(super) doc_key: MetadataDocKey,
}

pub(crate) type ReusedMetadataDocuments = HashMap<String, ReusedMetadataDocument>;

#[derive(Debug, Default)]
pub(super) struct CompactContractTokens {
    offsets: Box<[u64]>,
    values: Box<[u32]>,
}

impl CompactContractTokens {
    fn from_parts(offsets: Vec<u64>, values: Vec<u32>) -> Self {
        debug_assert!(!offsets.is_empty());
        debug_assert_eq!(offsets.last().copied().unwrap_or(0), values.len() as u64);
        Self {
            offsets: offsets.into_boxed_slice(),
            values: values.into_boxed_slice(),
        }
    }

    #[cfg(test)]
    pub(super) fn from_nested(mut nested: Vec<Vec<u32>>) -> Self {
        nested.iter_mut().for_each(|tokens| {
            tokens.sort_unstable();
            tokens.dedup();
        });
        let value_count = nested.iter().map(Vec::len).sum();
        let mut offsets = Vec::with_capacity(nested.len().saturating_add(1));
        let mut values = Vec::with_capacity(value_count);
        offsets.push(0);
        for tokens in nested {
            values.extend(tokens);
            offsets.push(values.len() as u64);
        }
        Self::from_parts(offsets, values)
    }

    pub(super) fn tokens(&self, index: usize) -> &[u32] {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        &self.values[start..end]
    }

    pub(super) fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn memory_bytes(&self) -> usize {
        self.offsets
            .len()
            .saturating_mul(std::mem::size_of::<u64>())
            .saturating_add(self.values.len().saturating_mul(std::mem::size_of::<u32>()))
    }
}

impl Index<usize> for CompactContractTokens {
    type Output = [u32];

    fn index(&self, index: usize) -> &Self::Output {
        self.tokens(index)
    }
}

#[derive(Clone, Copy)]
pub(super) struct MetadataLoadBudgets {
    builder_bytes: usize,
    transient_bytes: usize,
}

impl MetadataLoadBudgets {
    pub(super) fn new(builder_bytes: usize, transient_bytes: usize) -> Self {
        Self {
            builder_bytes,
            transient_bytes,
        }
    }
}

/// Raw rows are retained while Rayon parses JSON, normalizes strings and builds
/// three token collections plus a term-frequency map. Account for both raw and
/// indexed row/vector headers, then reserve 96x the raw payload for two BM25
/// documents, each of which can own the same high-cardinality terms in its
/// token Vec, unique-token Vec and frequency HashMap. Another 25% covers
/// allocator and parallel-collect slack. Cached
/// rows avoid parsing but clone the cached prefilter and document key, so that
/// exact clone payload is included when it is larger than the parse estimate.
pub(super) fn metadata_load_row_transient_bytes(
    chain: &str,
    metadata_json: &str,
    reused_document: Option<&ReusedMetadataDocument>,
) -> usize {
    metadata_load_row_transient_bytes_for_capacities(
        chain.len(),
        metadata_json.len(),
        reused_document,
    )
}

fn metadata_load_row_transient_bytes_for_capacities(
    chain_capacity: usize,
    metadata_capacity: usize,
    reused_document: Option<&ReusedMetadataDocument>,
) -> usize {
    let raw_payload_bytes = chain_capacity.saturating_add(metadata_capacity);
    let cached_clone_bytes = reused_document.map_or(0, |document| {
        document
            .prefilter
            .as_ref()
            .map_or(0, |prefilter| {
                std::mem::size_of::<MetadataBm25Document>().saturating_add(prefilter.memory_bytes())
            })
            .saturating_add(std::mem::size_of::<MetadataDocKey>())
            .saturating_add(document.doc_key.capacity())
            .saturating_add(std::mem::size_of::<Arc<MetadataBm25Document>>())
    });
    let row_and_vector_headers = std::mem::size_of::<(u32, RawMetadataRow)>()
        .saturating_mul(2)
        .saturating_add(std::mem::size_of::<Option<(u32, IndexedMetadataRow)>>().saturating_mul(2));
    metadata_uncached_parse_transient_bytes(raw_payload_bytes, row_and_vector_headers).max(
        metadata_cached_clone_transient_bytes(
            raw_payload_bytes,
            row_and_vector_headers,
            cached_clone_bytes,
        ),
    )
}

pub(super) fn metadata_load_transient_reserve_bytes(
    analysis_memory_bytes: usize,
    chains: &[String],
) -> Result<usize, AnalysisError> {
    let maximum_chain_bytes = chains.iter().map(String::len).max().unwrap_or(0);
    let single_maximum_row = metadata_load_row_transient_bytes_for_capacities(
        maximum_chain_bytes,
        MAX_METADATA_BYTES_FOR_DEDUP,
        None,
    );
    let full_chunk = single_maximum_row.saturating_mul(METADATA_LOAD_CHUNK_ROWS);
    let allowance = analysis_memory_bytes
        .saturating_div(METADATA_LOAD_TRANSIENT_BUDGET_DIVISOR)
        .min(METADATA_LOAD_TRANSIENT_MAX_BYTES)
        .min(full_chunk);
    if allowance < single_maximum_row {
        return Err(AnalysisError::InvalidData(format!(
            "metadata load needs at least {} bytes of transient parse budget for one maximum-size row, only {} bytes are available",
            single_maximum_row, allowance
        )));
    }
    Ok(allowance)
}

pub(super) struct MetadataLoadChunk {
    rows: Vec<(u32, RawMetadataRow)>,
    estimated_transient_bytes: usize,
    maximum_transient_bytes: usize,
}

impl MetadataLoadChunk {
    pub(super) fn new(maximum_transient_bytes: usize) -> Self {
        Self {
            rows: Vec::new(),
            estimated_transient_bytes: 0,
            maximum_transient_bytes,
        }
    }

    /// Returns `false` without copying either input when the existing chunk
    /// must be parsed first. A single row that cannot fit is rejected while the
    /// chunk is still unchanged, before JSON parsing or unbounded allocation.
    pub(super) fn try_push(
        &mut self,
        source_contract_index: u32,
        chain: &str,
        metadata_json: &str,
        nft_count: i64,
        reused_documents: &ReusedMetadataDocuments,
    ) -> Result<bool, AnalysisError> {
        let reused_document = reused_documents.get(metadata_json);
        let row_bytes = metadata_load_row_transient_bytes(chain, metadata_json, reused_document);
        if row_bytes > self.maximum_transient_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "single metadata row parse peak needs about {row_bytes} bytes, exceeding transient load budget {} bytes",
                self.maximum_transient_bytes
            )));
        }
        if !self.rows.is_empty()
            && (self.rows.len() >= METADATA_LOAD_CHUNK_ROWS
                || self.estimated_transient_bytes.saturating_add(row_bytes)
                    > self.maximum_transient_bytes)
        {
            return Ok(false);
        }

        self.rows.try_reserve(1).map_err(|_| {
            AnalysisError::InvalidData("unable to reserve bounded metadata load chunk".to_string())
        })?;
        let chain = chain.to_owned();
        let metadata_json = metadata_json.to_owned();
        let actual_row_bytes = metadata_load_row_transient_bytes_for_capacities(
            chain.capacity(),
            metadata_json.capacity(),
            reused_document,
        );
        let projected_bytes = self
            .estimated_transient_bytes
            .saturating_add(actual_row_bytes);
        if projected_bytes > self.maximum_transient_bytes {
            return Err(AnalysisError::InvalidData(format!(
                "metadata row allocation raised projected parse peak to {projected_bytes} bytes, exceeding transient load budget {} bytes",
                self.maximum_transient_bytes
            )));
        }
        self.rows.push((
            source_contract_index,
            RawMetadataRow {
                chain,
                metadata_json,
                nft_count,
            },
        ));
        self.estimated_transient_bytes = projected_bytes;
        Ok(true)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    fn take(&mut self) -> Vec<(u32, RawMetadataRow)> {
        self.estimated_transient_bytes = 0;
        std::mem::take(&mut self.rows)
    }
}

pub(super) fn hash_table_allocation_bytes(capacity: usize, entry_bytes: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    // std HashMap's current 7/8 maximum load means `capacity()` is smaller
    // than the raw bucket array. Include the control-byte group as well.
    let buckets = capacity.saturating_add(capacity.div_ceil(7));
    buckets
        .saturating_mul(entry_bytes)
        .saturating_add(buckets)
        .saturating_add(16)
}

pub(super) fn hash_table_allocation_for_len_upper(len: usize, entry_bytes: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let required_buckets = len
        .saturating_mul(8)
        .saturating_add(6)
        .saturating_div(7)
        .max(4);
    let buckets = required_buckets
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX);
    buckets
        .saturating_mul(entry_bytes)
        .saturating_add(buckets)
        .saturating_add(16)
}

pub(super) fn reused_metadata_documents_sql() -> &'static str {
    "
        WITH required_sources AS (
            SELECT metadata_source_file,
                   metadata_source_row_number
            FROM analysis_contracts
            WHERE metadata_source_file IS NOT NULL
            UNION
            SELECT metadata_source_file,
                   metadata_source_row_number
            FROM metadata_contract_token_rows
        )
        SELECT rows.metadata_json
        FROM required_sources sources
        JOIN metadata_rows rows
          ON rows.source_file = sources.metadata_source_file
         AND rows.source_row_number = sources.metadata_source_row_number
        GROUP BY rows.metadata_json
        HAVING count(*) >= 2
    "
}

pub(super) fn reused_metadata_documents_memory_bytes(documents: &ReusedMetadataDocuments) -> usize {
    documents.iter().fold(
        reused_metadata_documents_non_content_memory_bytes(documents),
        |bytes, (_, document)| {
            bytes.saturating_add(
                document
                    .content
                    .as_ref()
                    .map_or(0, metadata_content_arc_memory_bytes),
            )
        },
    )
}

pub(super) fn reused_metadata_documents_non_content_memory_bytes(
    documents: &ReusedMetadataDocuments,
) -> usize {
    let buckets = hash_table_allocation_bytes(
        documents.capacity(),
        std::mem::size_of::<(String, ReusedMetadataDocument)>(),
    );
    documents.iter().fold(buckets, |bytes, (raw, document)| {
        bytes
            .saturating_add(raw.capacity())
            .saturating_add(document.doc_key.capacity())
            .saturating_add(
                document
                    .prefilter
                    .as_ref()
                    .map_or(0, MetadataBm25Document::memory_bytes),
            )
    })
}

pub(super) fn metadata_content_arc_memory_bytes(document: &Arc<MetadataBm25Document>) -> usize {
    MetadataBm25Document::memory_bytes(document)
        .saturating_add(std::mem::size_of::<MetadataBm25Document>())
        .saturating_add(2usize.saturating_mul(std::mem::size_of::<usize>()))
}

fn reused_metadata_document_payload_bytes(
    raw_capacity: usize,
    document: &ReusedMetadataDocument,
) -> usize {
    let prefilter_bytes = document
        .prefilter
        .as_ref()
        .map_or(0, MetadataBm25Document::memory_bytes);
    let content_bytes = document
        .content
        .as_ref()
        .map_or(0, metadata_content_arc_memory_bytes);
    raw_capacity
        .saturating_add(document.doc_key.capacity())
        .saturating_add(prefilter_bytes)
        .saturating_add(content_bytes)
}

pub(super) fn load_reused_metadata_documents(
    conn: &Connection,
    pool: &rayon::ThreadPool,
    max_raw_bytes: Option<usize>,
) -> Result<ReusedMetadataDocuments, AnalysisError> {
    if max_raw_bytes == Some(0) {
        return Ok(ReusedMetadataDocuments::new());
    }
    let mut stmt = conn.prepare(reused_metadata_documents_sql())?;
    let mut documents = ReusedMetadataDocuments::new();
    // Keep the heap owned by keys and parsed documents as an incremental
    // scalar. Recomputing it by walking the whole cache before every insert
    // makes loading N reused documents O(N^2).
    let mut retained_payload_bytes = 0usize;
    for batch in stmt.query_arrow([])? {
        let metadata = arrow_string_column(&batch, 0, "metadata_json")?;
        let mut raw_documents = Vec::with_capacity(batch.num_rows());
        for row_index in 0..batch.num_rows() {
            raw_documents.push(metadata.value(row_index).to_owned());
        }
        let parsed = pool.install(|| {
            raw_documents
                .into_par_iter()
                .map(|raw| {
                    let documents = metadata_documents_from_json(&raw);
                    let prefilter = MetadataBm25Document::from_text(&documents.prefilter);
                    let content = MetadataBm25Document::from_text(&documents.content).map(Arc::new);
                    let cached = ReusedMetadataDocument {
                        prefilter,
                        content,
                        doc_key: metadata_document_key(&documents.prefilter),
                    };
                    (raw, cached)
                })
                .collect::<Vec<_>>()
        });
        for (raw, cached) in parsed {
            let candidate_payload_bytes =
                reused_metadata_document_payload_bytes(raw.capacity(), &cached);
            if let Some(maximum) = max_raw_bytes {
                documents.try_reserve(1).map_err(|_| {
                    AnalysisError::InvalidData(
                        "unable to reserve bounded reused metadata cache".to_string(),
                    )
                })?;
                let projected_bytes = hash_table_allocation_bytes(
                    documents.capacity(),
                    std::mem::size_of::<(String, ReusedMetadataDocument)>(),
                )
                .saturating_add(retained_payload_bytes)
                .saturating_add(candidate_payload_bytes);
                if projected_bytes > maximum {
                    documents.shrink_to_fit();
                    return Ok(documents);
                }
            }
            let previous = documents.insert(raw, cached);
            debug_assert!(previous.is_none());
            retained_payload_bytes = retained_payload_bytes.saturating_add(candidate_payload_bytes);
        }
    }
    Ok(documents)
}

pub(super) fn load_metadata_data(
    conn: &Connection,
    chains: &[String],
    pool: &rayon::ThreadPool,
    reused_documents: ReusedMetadataDocuments,
    budgets: MetadataLoadBudgets,
) -> Result<MetadataData, AnalysisError> {
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt = conn.prepare(&metadata_raw_rows_sql())?;
    let mut builder = MetadataDataBuilder::new(chains.len());
    let mut chunk = MetadataLoadChunk::new(budgets.transient_bytes);
    let mut source_count = 0usize;
    for batch in stmt.query_arrow([])? {
        let source_index_column = arrow_i64_column(&batch, 0, "metadata_contract_index")?;
        let chain_column = arrow_string_column(&batch, 1, "chain")?;
        let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
        let nft_count_column = arrow_i64_column(&batch, 3, "nft_count")?;
        for row_index in 0..batch.num_rows() {
            let source_contract_index = u32::try_from(source_index_column.value(row_index))
                .map_err(|_| {
                    AnalysisError::InvalidData(
                        "metadata source contract index exceeds u32 indexes".to_string(),
                    )
                })?;
            source_count = source_count.max(source_contract_index as usize + 1);
            let chain = chain_column.value(row_index);
            let metadata_json = metadata_column.value(row_index);
            let nft_count = nft_count_column.value(row_index);
            if !chunk.try_push(
                source_contract_index,
                chain,
                metadata_json,
                nft_count,
                &reused_documents,
            )? {
                merge_metadata_load_chunk(
                    &mut chunk,
                    &chain_indexes,
                    pool,
                    &mut builder,
                    &reused_documents,
                    budgets.builder_bytes,
                )?;
                let added = chunk.try_push(
                    source_contract_index,
                    chain,
                    metadata_json,
                    nft_count,
                    &reused_documents,
                )?;
                if !added {
                    return Err(AnalysisError::InvalidData(
                        "empty bounded metadata chunk rejected a fitting row".to_string(),
                    ));
                }
            }
        }
    }

    merge_metadata_load_chunk(
        &mut chunk,
        &chain_indexes,
        pool,
        &mut builder,
        &reused_documents,
        budgets.builder_bytes,
    )?;

    let missing_source_indexes = builder.missing_source_indexes(source_count);
    if !missing_source_indexes.is_empty() {
        load_metadata_fallback_rows(
            conn,
            &missing_source_indexes,
            &chain_indexes,
            pool,
            &mut builder,
            &reused_documents,
            budgets,
        )?;
    }

    Ok(pool.install(|| builder.finish_with_reused_documents(reused_documents)))
}

fn index_metadata_load_chunk(
    chunk: &mut MetadataLoadChunk,
    chain_indexes: &HashMap<&str, usize>,
    pool: &rayon::ThreadPool,
    builder: &MetadataDataBuilder,
    reused_documents: &ReusedMetadataDocuments,
    maximum_builder_bytes: usize,
) -> Result<Vec<(u32, IndexedMetadataRow)>, AnalysisError> {
    if chunk.is_empty() {
        return Ok(Vec::new());
    }
    // The builder budget excludes the entire transient allowance, so this
    // guard must run before parallel parsing starts, not only after merging.
    builder.ensure_within_memory_budget(maximum_builder_bytes)?;
    let raw_rows = chunk.take();
    Ok(pool.install(|| {
        index_metadata_raw_row_chunk_with_cache(raw_rows, chain_indexes, reused_documents)
    }))
}

fn merge_metadata_load_chunk(
    chunk: &mut MetadataLoadChunk,
    chain_indexes: &HashMap<&str, usize>,
    pool: &rayon::ThreadPool,
    builder: &mut MetadataDataBuilder,
    reused_documents: &ReusedMetadataDocuments,
    maximum_builder_bytes: usize,
) -> Result<(), AnalysisError> {
    let indexed_rows = index_metadata_load_chunk(
        chunk,
        chain_indexes,
        pool,
        builder,
        reused_documents,
        maximum_builder_bytes,
    )?;
    builder.merge_indexed_rows(indexed_rows);
    builder.ensure_within_memory_budget(maximum_builder_bytes)
}

pub(super) fn load_metadata_fallback_rows(
    conn: &Connection,
    missing_source_indexes: &[u32],
    chain_indexes: &HashMap<&str, usize>,
    pool: &rayon::ThreadPool,
    builder: &mut MetadataDataBuilder,
    reused_documents: &ReusedMetadataDocuments,
    budgets: MetadataLoadBudgets,
) -> Result<(), AnalysisError> {
    conn.execute_batch(&format!(
        "
        CREATE OR REPLACE TEMP TABLE {METADATA_FALLBACK_SOURCE_TABLE} (
            source_index BIGINT PRIMARY KEY
        );
        "
    ))?;
    {
        let mut appender = conn.appender(METADATA_FALLBACK_SOURCE_TABLE)?;
        for &source_index in missing_source_indexes {
            appender.append_row([i64::from(source_index)])?;
        }
        appender.flush()?;
    }

    let sql = format!(
        "
        SELECT c.metadata_contract_index,
               c.chain,
               r.metadata_json,
               c.nft_count
        FROM analysis_contracts c
        JOIN {METADATA_FALLBACK_SOURCE_TABLE} f
          ON f.source_index = c.metadata_contract_index
        JOIN metadata_rows r
          ON r.contract_id = c.contract_id
        WHERE r.metadata_eligible
        ORDER BY c.metadata_contract_index, r.token_id, r.source_file, r.source_row_number
        "
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut resolved = HashSet::<u32>::new();
    let mut chunk = MetadataLoadChunk::new(budgets.transient_bytes);
    for batch in stmt.query_arrow([])? {
        let source_index_column = arrow_i64_column(&batch, 0, "metadata_contract_index")?;
        let chain_column = arrow_string_column(&batch, 1, "chain")?;
        let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
        let nft_count_column = arrow_i64_column(&batch, 3, "nft_count")?;
        for row_index in 0..batch.num_rows() {
            let source_contract_index = u32::try_from(source_index_column.value(row_index))
                .map_err(|_| {
                    AnalysisError::InvalidData(
                        "metadata source contract index exceeds u32 indexes".to_string(),
                    )
                })?;
            if resolved.contains(&source_contract_index) {
                continue;
            }
            let chain = chain_column.value(row_index);
            let metadata_json = metadata_column.value(row_index);
            let nft_count = nft_count_column.value(row_index);
            if !chunk.try_push(
                source_contract_index,
                chain,
                metadata_json,
                nft_count,
                reused_documents,
            )? {
                merge_metadata_fallback_load_chunk(
                    &mut chunk,
                    chain_indexes,
                    pool,
                    builder,
                    reused_documents,
                    budgets.builder_bytes,
                    &mut resolved,
                )?;
                if !resolved.contains(&source_contract_index) {
                    let added = chunk.try_push(
                        source_contract_index,
                        chain,
                        metadata_json,
                        nft_count,
                        reused_documents,
                    )?;
                    if !added {
                        return Err(AnalysisError::InvalidData(
                            "empty bounded metadata fallback chunk rejected a fitting row"
                                .to_string(),
                        ));
                    }
                }
            }
        }
    }
    merge_metadata_fallback_load_chunk(
        &mut chunk,
        chain_indexes,
        pool,
        builder,
        reused_documents,
        budgets.builder_bytes,
        &mut resolved,
    )?;
    drop(stmt);
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {METADATA_FALLBACK_SOURCE_TABLE};"
    ))?;
    Ok(())
}

fn merge_metadata_fallback_load_chunk(
    chunk: &mut MetadataLoadChunk,
    chain_indexes: &HashMap<&str, usize>,
    pool: &rayon::ThreadPool,
    builder: &mut MetadataDataBuilder,
    reused_documents: &ReusedMetadataDocuments,
    maximum_builder_bytes: usize,
    resolved: &mut HashSet<u32>,
) -> Result<(), AnalysisError> {
    let indexed_rows = index_metadata_load_chunk(
        chunk,
        chain_indexes,
        pool,
        builder,
        reused_documents,
        maximum_builder_bytes,
    )?;
    let mut first_rows = Vec::new();
    for (source_contract_index, row) in indexed_rows {
        if resolved.insert(source_contract_index) {
            first_rows.push((source_contract_index, row));
        }
    }
    builder.merge_indexed_rows(first_rows);
    builder.ensure_within_memory_budget(maximum_builder_bytes)
}

pub(crate) fn metadata_raw_rows_sql() -> String {
    "
        SELECT contracts.metadata_contract_index,
               contracts.chain,
               rows.metadata_json,
               contracts.nft_count
        FROM analysis_contracts contracts
        JOIN metadata_rows rows
          ON rows.source_file = contracts.metadata_source_file
         AND rows.source_row_number = contracts.metadata_source_row_number
        WHERE contracts.metadata_contract_index IS NOT NULL
        ORDER BY contracts.metadata_contract_index
    "
    .to_string()
}

#[cfg(test)]
pub(super) fn index_metadata_raw_row_chunk(
    raw_rows: Vec<(u32, RawMetadataRow)>,
    chain_indexes: &HashMap<&str, usize>,
) -> Vec<(u32, IndexedMetadataRow)> {
    index_metadata_raw_row_chunk_with_cache(raw_rows, chain_indexes, &HashMap::new())
}

pub(super) fn index_metadata_raw_row_chunk_with_cache(
    raw_rows: Vec<(u32, RawMetadataRow)>,
    chain_indexes: &HashMap<&str, usize>,
    reused_documents: &ReusedMetadataDocuments,
) -> Vec<(u32, IndexedMetadataRow)> {
    let indexed = raw_rows
        .into_par_iter()
        .map(|(source_contract_index, row)| {
            let chain_index = chain_indexes.get(row.chain.as_str()).copied()?;
            if !metadata_is_dedup_eligible(&row.metadata_json) {
                return None;
            }
            let (doc, content_doc, doc_key) = if let Some(cached) =
                reused_documents.get(&row.metadata_json)
            {
                (
                    cached.prefilter.clone()?,
                    cached.content.clone(),
                    cached.doc_key.clone(),
                )
            } else {
                let documents = metadata_documents_from_json(&row.metadata_json);
                let doc = MetadataBm25Document::from_text(&documents.prefilter)?;
                let doc_key = metadata_document_key(&documents.prefilter);
                let content_doc = MetadataBm25Document::from_text(&documents.content).map(Arc::new);
                (doc, content_doc, doc_key)
            };
            Some((
                source_contract_index,
                IndexedMetadataRow {
                    chain_index,
                    nft_count: row.nft_count,
                    content_doc,
                    doc,
                    doc_key,
                },
            ))
        })
        .collect::<Vec<_>>();
    indexed.into_iter().flatten().collect()
}

pub(super) fn metadata_document_key(document: &str) -> MetadataDocKey {
    document.to_string()
}

#[cfg(test)]
pub(super) fn prepare_metadata_contract_token_rows(conn: &Connection) -> Result<(), AnalysisError> {
    conn.execute_batch(metadata_contract_token_rows_sql())?;
    Ok(())
}

pub(super) fn metadata_contract_token_rows_sql() -> &'static str {
    "
        DROP TABLE IF EXISTS metadata_contract_token_rows;
        DROP TABLE IF EXISTS metadata_token_stats;
        CREATE TEMP TABLE metadata_unique_contract_tokens AS
        SELECT c.metadata_contract_index AS contract_index,
               a.token_id,
               arg_min(
                   struct_pack(
                       file_id := a.source_file,
                       row_number := a.source_row_number
                   ),
                   row(a.source_file, a.source_row_number)
               ) AS metadata_source
        FROM metadata_rows a
        JOIN analysis_contracts c
          ON c.contract_id = a.contract_id
        WHERE a.token_id <> ''
          AND c.metadata_contract_index IS NOT NULL
          AND a.metadata_eligible
        GROUP BY c.metadata_contract_index, a.token_id;

        CREATE TEMP TABLE metadata_token_frequencies AS
        SELECT token_id, count(*)::UBIGINT AS contract_frequency
        FROM metadata_unique_contract_tokens
        GROUP BY token_id;

        CREATE TABLE metadata_token_stats AS
        SELECT count(*) FILTER (WHERE contract_frequency = 1)::UBIGINT
                   AS singleton_token_count,
               count(*) FILTER (WHERE contract_frequency >= 2)::UBIGINT
                   AS retained_shared_token_count
        FROM metadata_token_frequencies;

        CREATE TABLE metadata_contract_token_rows AS
        SELECT metadata.contract_index,
               (dense_rank() OVER (ORDER BY metadata.token_id) - 1)::BIGINT AS token_index,
               metadata.metadata_source.file_id::UINTEGER AS metadata_source_file,
               metadata.metadata_source.row_number::UBIGINT AS metadata_source_row_number
        FROM metadata_unique_contract_tokens metadata
        INNER JOIN metadata_token_frequencies frequencies USING (token_id)
        WHERE frequencies.contract_frequency >= 2;

        DROP TABLE metadata_unique_contract_tokens;
        DROP TABLE metadata_token_frequencies;
    "
}

pub(super) fn load_metadata_contract_tokens(
    conn: &Connection,
    data: &MetadataData,
    pool: &rayon::ThreadPool,
) -> Result<CompactContractTokens, AnalysisError> {
    let contract_count = data.contracts.len();
    // One u32 per compact contract serves first as the row count and then as
    // the second-pass cursor. Reusing it avoids another O(contract_count)
    // allocation while the final u64 offsets and u32 values are resident.
    let mut counts_and_cursors = vec![0u32; contract_count];
    {
        let mut stmt = conn.prepare(
            "
            SELECT contract_index
            FROM metadata_contract_token_rows
            ",
        )?;
        for batch in stmt.query_arrow([])? {
            let contract_column = arrow_i64_column(&batch, 0, "contract_index")?;
            for row_index in 0..batch.num_rows() {
                let source_contract_index = usize::try_from(contract_column.value(row_index))
                    .map_err(|_| {
                        AnalysisError::InvalidData("negative metadata contract index".to_string())
                    })?;
                let Some(contract_index) =
                    data.compact_contract_index_for_source(source_contract_index)
                else {
                    continue;
                };
                let contract_index = metadata_contract_index_to_usize(contract_index);
                counts_and_cursors[contract_index] = counts_and_cursors[contract_index]
                    .checked_add(1)
                    .ok_or_else(|| {
                        AnalysisError::InvalidData(
                            "one metadata contract has more than u32::MAX shared tokens"
                                .to_string(),
                        )
                    })?;
            }
        }
    }

    let mut offsets = Vec::with_capacity(contract_count.saturating_add(1));
    offsets.push(0u64);
    for &count in &counts_and_cursors {
        let next = offsets
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(u64::from(count))
            .ok_or_else(|| {
                AnalysisError::InvalidData(
                    "metadata contract-token offsets exceed u64 indexes".to_string(),
                )
            })?;
        offsets.push(next);
    }
    let value_count = usize::try_from(offsets.last().copied().unwrap_or(0)).map_err(|_| {
        AnalysisError::InvalidData(
            "metadata contract-token values exceed addressable memory".to_string(),
        )
    })?;
    let mut values = vec![0u32; value_count];
    counts_and_cursors.fill(0);

    {
        let mut stmt = conn.prepare(
            "
            SELECT contract_index, token_index
            FROM metadata_contract_token_rows
            ",
        )?;
        for batch in stmt.query_arrow([])? {
            let contract_column = arrow_i64_column(&batch, 0, "contract_index")?;
            let token_column = arrow_i64_column(&batch, 1, "token_index")?;
            for row_index in 0..batch.num_rows() {
                let source_contract_index = usize::try_from(contract_column.value(row_index))
                    .map_err(|_| {
                        AnalysisError::InvalidData("negative metadata contract index".to_string())
                    })?;
                let Some(contract_index) =
                    data.compact_contract_index_for_source(source_contract_index)
                else {
                    continue;
                };
                let contract_index = metadata_contract_index_to_usize(contract_index);
                let token_index = u32::try_from(token_column.value(row_index)).map_err(|_| {
                    AnalysisError::InvalidData(
                        "metadata token dictionary exceeds compact u32 indexes".to_string(),
                    )
                })?;
                let cursor = &mut counts_and_cursors[contract_index];
                let position = offsets[contract_index].saturating_add(u64::from(*cursor));
                if position >= offsets[contract_index + 1] {
                    return Err(AnalysisError::InvalidData(
                        "metadata contract-token rows changed between loading passes".to_string(),
                    ));
                }
                values[position as usize] = token_index;
                *cursor = cursor.checked_add(1).ok_or_else(|| {
                    AnalysisError::InvalidData(
                        "one metadata contract has more than u32::MAX shared tokens".to_string(),
                    )
                })?;
            }
        }
    }
    for (contract_index, &cursor) in counts_and_cursors.iter().enumerate() {
        let expected = offsets[contract_index + 1] - offsets[contract_index];
        if u64::from(cursor) != expected {
            return Err(AnalysisError::InvalidData(
                "metadata contract-token rows changed between loading passes".to_string(),
            ));
        }
    }
    pool.install(|| sort_compact_contract_token_slices(&offsets, &mut values));
    drop(counts_and_cursors);
    Ok(CompactContractTokens::from_parts(offsets, values))
}

const CONTRACT_TOKEN_SORT_LEAF_CONTRACTS: usize = 4 * 1024;

fn sort_compact_contract_token_slices(offsets: &[u64], values: &mut [u32]) {
    sort_compact_contract_token_range(offsets, values, offsets.first().copied().unwrap_or(0));
}

fn sort_compact_contract_token_range(offsets: &[u64], values: &mut [u32], base_offset: u64) {
    let contract_count = offsets.len().saturating_sub(1);
    if contract_count == 0 || values.is_empty() {
        return;
    }
    if contract_count <= CONTRACT_TOKEN_SORT_LEAF_CONTRACTS {
        for range in offsets.windows(2) {
            let start = (range[0] - base_offset) as usize;
            let end = (range[1] - base_offset) as usize;
            values[start..end].sort_unstable();
        }
        return;
    }

    let middle_contract = contract_count / 2;
    let middle_offset = offsets[middle_contract];
    let split_at = (middle_offset - base_offset) as usize;
    let (left_values, right_values) = values.split_at_mut(split_at);
    let right_offsets = &offsets[middle_contract..];
    rayon::join(
        || {
            sort_compact_contract_token_range(
                &offsets[..=middle_contract],
                left_values,
                base_offset,
            )
        },
        || sort_compact_contract_token_range(right_offsets, right_values, middle_offset),
    );
}
