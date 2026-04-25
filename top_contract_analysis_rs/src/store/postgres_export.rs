use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use once_cell::sync::Lazy;
use parquet::arrow::ArrowWriter;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::Client;
use regex::Regex;

use crate::analysis::scoring::metadata_document_from_json;
use crate::error::AppError;
use crate::normalize::{normalize_name, normalize_symbol, normalize_url};

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotExportRow {
    pub contract_address: String,
    pub token_id: String,
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub symbol: String,
    pub metadata_json: String,
}

fn chain_to_table(chain: &str) -> Result<String, AppError> {
    let safe: String = chain
        .trim()
        .to_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || *ch == '_')
        .collect();
    if safe.is_empty() {
        return Err(AppError::InvalidData(format!(
            "illegal chain name: {chain:?}"
        )));
    }
    Ok(format!("nft_assets_{safe}"))
}

fn metadata_keywords(document: &str, limit: usize) -> Vec<String> {
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for token in TOKEN_RE.find_iter(document) {
        let normalized = token.as_str().to_lowercase();
        if normalized.len() < 4 {
            continue;
        }
        *counts.entry(normalized).or_insert(0) += 1;
    }
    let mut ranked: Vec<(String, usize)> = counts.into_iter().collect();
    ranked.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| right.0.len().cmp(&left.0.len()))
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .take(limit)
        .map(|(token, _)| token)
        .collect()
}

fn snapshot_schema(keep_metadata_json: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("chain", DataType::Utf8, false),
        Field::new("contract_address", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("token_uri", DataType::Utf8, false),
        Field::new("image_uri", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
    ];
    if keep_metadata_json {
        fields.push(Field::new("metadata_json", DataType::Utf8, false));
    }
    fields.extend([
        Field::new("token_uri_norm", DataType::Utf8, false),
        Field::new("image_uri_norm", DataType::Utf8, false),
        Field::new("name_norm", DataType::Utf8, false),
        Field::new("symbol_norm", DataType::Utf8, false),
        Field::new("metadata_doc", DataType::Utf8, false),
        Field::new("metadata_keywords_arr", DataType::Utf8, false),
    ]);
    Arc::new(Schema::new(fields))
}

fn snapshot_batch(
    chain: &str,
    rows: &[SnapshotExportRow],
    keep_metadata_json: bool,
) -> Result<RecordBatch, AppError> {
    let schema = snapshot_schema(keep_metadata_json);
    let chain_values: Vec<String> = rows.iter().map(|_| chain.to_string()).collect();
    let contract_address_values: Vec<String> = rows
        .iter()
        .map(|row| row.contract_address.to_lowercase())
        .collect();
    let token_id_values: Vec<String> = rows.iter().map(|row| row.token_id.clone()).collect();
    let token_uri_values: Vec<String> = rows.iter().map(|row| row.token_uri.clone()).collect();
    let image_uri_values: Vec<String> = rows.iter().map(|row| row.image_uri.clone()).collect();
    let name_values: Vec<String> = rows.iter().map(|row| row.name.clone()).collect();
    let symbol_values: Vec<String> = rows.iter().map(|row| row.symbol.clone()).collect();
    let metadata_json_values: Vec<String> =
        rows.iter().map(|row| row.metadata_json.clone()).collect();
    let metadata_doc_values: Vec<String> = rows
        .iter()
        .map(|row| metadata_document_from_json(&row.metadata_json))
        .collect();
    let metadata_keyword_values: Vec<String> = metadata_doc_values
        .iter()
        .map(|doc| serde_json::to_string(&metadata_keywords(doc, 8)))
        .collect::<Result<_, _>>()?;

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(chain_values)),
        Arc::new(StringArray::from(contract_address_values)),
        Arc::new(StringArray::from(token_id_values)),
        Arc::new(StringArray::from(token_uri_values.clone())),
        Arc::new(StringArray::from(image_uri_values.clone())),
        Arc::new(StringArray::from(name_values.clone())),
        Arc::new(StringArray::from(symbol_values.clone())),
    ];
    if keep_metadata_json {
        columns.push(Arc::new(StringArray::from(metadata_json_values.clone())));
    }
    columns.extend([
        Arc::new(StringArray::from(
            token_uri_values
                .iter()
                .map(|value| normalize_url(value).unwrap_or_default())
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            image_uri_values
                .iter()
                .map(|value| normalize_url(value).unwrap_or_default())
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            name_values
                .iter()
                .map(|value| normalize_name(value))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            symbol_values
                .iter()
                .map(|value| normalize_symbol(value))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(metadata_doc_values)) as ArrayRef,
        Arc::new(StringArray::from(metadata_keyword_values)) as ArrayRef,
    ]);

    RecordBatch::try_new(schema, columns).map_err(|err| AppError::InvalidData(err.to_string()))
}

fn open_snapshot_writer(
    output_path: &Path,
    keep_metadata_json: bool,
) -> Result<ArrowWriter<std::fs::File>, AppError> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(output_path)?;
    ArrowWriter::try_new(file, snapshot_schema(keep_metadata_json), None)
        .map_err(|err| AppError::InvalidData(err.to_string()))
}

pub fn write_snapshot_rows_to_parquet(
    chain: &str,
    rows: &[SnapshotExportRow],
    output_path: &Path,
    keep_metadata_json: bool,
) -> Result<(), AppError> {
    let mut writer = open_snapshot_writer(output_path, keep_metadata_json)?;
    let batch = snapshot_batch(chain, rows, keep_metadata_json)?;
    writer
        .write(&batch)
        .map_err(|err| AppError::InvalidData(err.to_string()))?;
    writer
        .close()
        .map_err(|err| AppError::InvalidData(err.to_string()))?;
    Ok(())
}

pub fn export_chain_snapshot_to_parquet(
    conn: &mut Client,
    chain: &str,
    output_path: &Path,
    fetch_size: usize,
    keep_metadata_json: bool,
) -> Result<(), AppError> {
    let table = chain_to_table(chain)?;
    let metadata_row = conn.query_opt(
        "
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1
          AND column_name IN ('raw_metadata', 'metadata')
        ORDER BY CASE WHEN column_name = 'raw_metadata' THEN 0 ELSE 1 END
        LIMIT 1
        ",
        &[&table],
    )?;
    let metadata_column = metadata_row
        .and_then(|row| row.try_get::<_, String>(0).ok())
        .unwrap_or_else(|| "metadata".to_string());

    let query = format!(
        "
        SELECT lower(contract_address), token_id::text, coalesce(token_uri, ''), coalesce(image_uri, ''),
               coalesce(name, ''), coalesce(symbol, ''), coalesce({metadata_column}::text, '')
        FROM {table}
        ORDER BY id
        "
    );
    let mut writer = open_snapshot_writer(output_path, keep_metadata_json)?;
    let mut rows = conn.query_raw(&query, std::iter::empty::<&(dyn ToSql + Sync)>())?;
    let batch_size = fetch_size.max(1);
    let mut buffer: Vec<SnapshotExportRow> = Vec::with_capacity(batch_size);
    while let Some(row) = rows.next()? {
        buffer.push(SnapshotExportRow {
            contract_address: row.get::<_, String>(0),
            token_id: row.get::<_, String>(1),
            token_uri: row.get::<_, String>(2),
            image_uri: row.get::<_, String>(3),
            name: row.get::<_, String>(4),
            symbol: row.get::<_, String>(5),
            metadata_json: row.get::<_, String>(6),
        });
        if buffer.len() >= batch_size {
            let batch = snapshot_batch(chain, &buffer, keep_metadata_json)?;
            writer
                .write(&batch)
                .map_err(|err| AppError::InvalidData(err.to_string()))?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        let batch = snapshot_batch(chain, &buffer, keep_metadata_json)?;
        writer
            .write(&batch)
            .map_err(|err| AppError::InvalidData(err.to_string()))?;
    }
    writer
        .close()
        .map_err(|err| AppError::InvalidData(err.to_string()))?;
    Ok(())
}
