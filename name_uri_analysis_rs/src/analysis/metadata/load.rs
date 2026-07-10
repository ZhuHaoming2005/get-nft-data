use std::collections::{HashMap, HashSet};

use duckdb::Connection;
use rayon::prelude::*;

use super::bm25::MetadataBm25Document;
use super::parse::{metadata_documents_from_json, metadata_is_dedup_eligible};
use super::super::{
    arrow_i64_column, arrow_string_column, metadata_json_eligible_predicate, AnalysisError,
};
use super::{
    metadata_contract_index_to_usize, MetadataData, MetadataDataBuilder, MetadataDocKey,
};

pub(super) const METADATA_LOAD_CHUNK_ROWS: usize = 16 * 1024;
pub(super) const METADATA_FALLBACK_SOURCE_TABLE: &str = "__metadata_fallback_source_indexes";

pub(super) struct RawMetadataRow {
    pub(super) chain: String,
    pub(super) metadata_json: String,
    pub(super) nft_count: i64,
}

pub(crate) struct IndexedMetadataRow {
    pub(super) chain_index: usize,
    pub(super) nft_count: i64,
    pub(super) content_document: String,
    pub(super) doc: MetadataBm25Document,
    pub(super) doc_key: MetadataDocKey,
}

pub(super) fn load_metadata_data(
    conn: &Connection,
    chains: &[String],
    pool: &rayon::ThreadPool,
) -> Result<MetadataData, AnalysisError> {
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt = conn.prepare(&metadata_raw_rows_sql())?;
    let mut builder = MetadataDataBuilder::new(chains.len());
    let mut raw_rows = Vec::with_capacity(METADATA_LOAD_CHUNK_ROWS);
    let mut source_count = 0usize;
    for batch in stmt.query_arrow([])? {
        let source_index_column =
            arrow_i64_column(&batch, 0, "metadata_contract_index")?;
        let chain_column = arrow_string_column(&batch, 1, "chain")?;
        let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
        let nft_count_column = arrow_i64_column(&batch, 3, "nft_count")?;
        for row_index in 0..batch.num_rows() {
            let source_contract_index = u32::try_from(
                source_index_column.value(row_index),
            )
            .map_err(|_| {
                AnalysisError::InvalidData(
                    "metadata source contract index exceeds u32 indexes"
                        .to_string(),
                )
            })?;
            source_count = source_count.max(source_contract_index as usize + 1);
            raw_rows.push((
                source_contract_index,
                RawMetadataRow {
                    chain: chain_column.value(row_index).to_owned(),
                    metadata_json: metadata_column.value(row_index).to_owned(),
                    nft_count: nft_count_column.value(row_index),
                },
            ));
            if raw_rows.len() >= METADATA_LOAD_CHUNK_ROWS {
                let chunk = std::mem::replace(
                    &mut raw_rows,
                    Vec::with_capacity(METADATA_LOAD_CHUNK_ROWS),
                );
                builder.merge_indexed_rows(pool.install(|| {
                    index_metadata_raw_row_chunk(chunk, &chain_indexes)
                }));
            }
        }
    }

    if !raw_rows.is_empty() {
        builder.merge_indexed_rows(pool.install(|| {
            index_metadata_raw_row_chunk(raw_rows, &chain_indexes)
        }));
    }

    let missing_source_indexes = builder.missing_source_indexes(source_count);
    if !missing_source_indexes.is_empty() {
        load_metadata_fallback_rows(
            conn,
            &missing_source_indexes,
            &chain_indexes,
            pool,
            &mut builder,
        )?;
    }

    Ok(pool.install(|| builder.finish()))
}

pub(super) fn load_metadata_fallback_rows(
    conn: &Connection,
    missing_source_indexes: &[u32],
    chain_indexes: &HashMap<&str, usize>,
    pool: &rayon::ThreadPool,
    builder: &mut MetadataDataBuilder,
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
        JOIN analysis_rows r
          ON r.chain = c.chain
         AND r.contract_address = c.contract_address
        WHERE {}
        ORDER BY c.metadata_contract_index, r.token_id, r.rowid
        ",
        metadata_json_eligible_predicate("r.metadata_json")
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut resolved = HashSet::<u32>::new();
    for batch in stmt.query_arrow([])? {
        let source_index_column =
            arrow_i64_column(&batch, 0, "metadata_contract_index")?;
        let chain_column = arrow_string_column(&batch, 1, "chain")?;
        let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
        let nft_count_column = arrow_i64_column(&batch, 3, "nft_count")?;
        let mut raw_rows = Vec::with_capacity(batch.num_rows());
        for row_index in 0..batch.num_rows() {
            let source_contract_index =
                u32::try_from(source_index_column.value(row_index)).map_err(|_| {
                    AnalysisError::InvalidData(
                        "metadata source contract index exceeds u32 indexes".to_string(),
                    )
                })?;
            if resolved.contains(&source_contract_index) {
                continue;
            }
            raw_rows.push((
                source_contract_index,
                RawMetadataRow {
                    chain: chain_column.value(row_index).to_owned(),
                    metadata_json: metadata_column.value(row_index).to_owned(),
                    nft_count: nft_count_column.value(row_index),
                },
            ));
        }
        let indexed_rows =
            pool.install(|| index_metadata_raw_row_chunk(raw_rows, chain_indexes));
        let mut first_rows = Vec::new();
        for (source_contract_index, row) in indexed_rows {
            if resolved.insert(source_contract_index) {
                first_rows.push((source_contract_index, row));
            }
        }
        builder.merge_indexed_rows(first_rows);
    }
    drop(stmt);
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {METADATA_FALLBACK_SOURCE_TABLE};"
    ))?;
    Ok(())
}

pub(crate) fn metadata_raw_rows_sql() -> String {
    "
        SELECT metadata_contract_index,
               chain,
               metadata_json,
               nft_count
        FROM analysis_contracts
        WHERE metadata_contract_index IS NOT NULL
        ORDER BY metadata_contract_index
    "
    .to_string()
}

pub(super) fn index_metadata_raw_row_chunk(
    raw_rows: Vec<(u32, RawMetadataRow)>,
    chain_indexes: &HashMap<&str, usize>,
) -> Vec<(u32, IndexedMetadataRow)> {
    raw_rows
        .into_par_iter()
        .filter_map(|(source_contract_index, row)| {
            let chain_index = chain_indexes.get(row.chain.as_str()).copied()?;
            if !metadata_is_dedup_eligible(&row.metadata_json) {
                return None;
            }
            let documents = metadata_documents_from_json(&row.metadata_json);
            let prefilter_document = documents.prefilter;
            let content_document = documents.content;
            let doc = MetadataBm25Document::from_text(&prefilter_document)?;
            let doc_key = metadata_document_key(&prefilter_document);
            Some((
                source_contract_index,
                IndexedMetadataRow {
                    chain_index,
                    nft_count: row.nft_count,
                    content_document,
                    doc,
                    doc_key,
                },
            ))
        })
        .collect()
}


pub(super) fn metadata_document_key(document: &str) -> MetadataDocKey {
    document.to_string()
}

pub(super) fn prepare_metadata_contract_token_rows(
    conn: &Connection,
) -> Result<(), AnalysisError> {
    conn.execute_batch(
        &format!(
            "
        DROP TABLE IF EXISTS metadata_contract_token_rows;
        CREATE TEMP TABLE metadata_contract_token_rows AS
        WITH unique_metadata AS (
            SELECT c.metadata_contract_index AS contract_index,
                   a.token_id,
                   min(a.rowid)::BIGINT AS metadata_row_id
            FROM analysis_rows a
            JOIN analysis_contracts c
              ON c.chain = a.chain
             AND c.contract_address = a.contract_address
            WHERE a.token_id <> ''
              AND c.metadata_contract_index IS NOT NULL
              AND {eligible}
            GROUP BY c.metadata_contract_index, a.token_id
        )
        SELECT contract_index,
               (dense_rank() OVER (ORDER BY token_id) - 1)::BIGINT AS token_index,
               metadata_row_id
        FROM unique_metadata;
        ",
            eligible = metadata_json_eligible_predicate("a.metadata_json"),
        ),
    )?;
    Ok(())
}

pub(super) fn load_metadata_contract_tokens(
    conn: &Connection,
    data: &MetadataData,
) -> Result<Vec<Vec<u32>>, AnalysisError> {
    let mut contract_tokens = vec![Vec::new(); data.contracts.len()];
    let mut stmt = conn.prepare(
        "
        SELECT contract_index, token_index
        FROM metadata_contract_token_rows
        ORDER BY contract_index, token_index
        ",
    )?;
    for batch in stmt.query_arrow([])? {
        let contract_column = arrow_i64_column(&batch, 0, "contract_index")?;
        let token_column = arrow_i64_column(&batch, 1, "token_index")?;
        for row_index in 0..batch.num_rows() {
            let source_contract_index =
                usize::try_from(contract_column.value(row_index)).map_err(|_| {
                    AnalysisError::InvalidData(
                        "negative metadata contract index".to_string(),
                    )
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
            let tokens = &mut contract_tokens[contract_index];
            tokens.push(token_index);
        }
    }
    Ok(contract_tokens)
}
