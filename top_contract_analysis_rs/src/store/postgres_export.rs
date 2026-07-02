use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::Client;

use crate::error::AppError;
use crate::normalize::{normalize_name, normalize_url};

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotBlockRange {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

fn normalized_chain(chain: &str) -> String {
    chain.trim().to_lowercase()
}

fn is_solana_chain(chain: &str) -> bool {
    chain.trim().eq_ignore_ascii_case("solana")
}

impl SnapshotBlockRange {
    pub fn new(start: Option<i64>, end: Option<i64>) -> Result<Self, AppError> {
        if start.is_some_and(|value| value < 0) || end.is_some_and(|value| value < 0) {
            return Err(AppError::InvalidData(
                "snapshot block bounds must be non-negative".to_string(),
            ));
        }
        if matches!((start, end), (Some(start), Some(end)) if start > end) {
            return Err(AppError::InvalidData(
                "snapshot start block must not exceed end block".to_string(),
            ));
        }
        Ok(Self { start, end })
    }

    pub fn validate_for_chain(self, chain: &str) -> Result<Self, AppError> {
        if is_solana_chain(chain) && (self.start.is_some() || self.end.is_some()) {
            return Err(AppError::InvalidData(
                "Solana block filtering is unavailable because current rows use first_seen_block = 0"
                    .to_string(),
            ));
        }
        Ok(self)
    }
}

struct SnapshotQuery {
    sql: String,
    params: Vec<i64>,
}

fn canonical_contract_address(chain: &str, address: &str) -> String {
    let trimmed = address.trim();
    if is_solana_chain(chain) {
        trimmed.to_string()
    } else {
        trimmed.to_lowercase()
    }
}

fn chain_to_table(chain: &str) -> Result<String, AppError> {
    let normalized = normalized_chain(chain);
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(AppError::InvalidData(format!(
            "illegal chain name: {chain:?}"
        )));
    }
    Ok(format!("nft_assets_{normalized}"))
}

fn build_snapshot_query(
    table: &str,
    metadata_column: &str,
    block_range: SnapshotBlockRange,
) -> SnapshotQuery {
    let mut predicates = Vec::new();
    let mut params = Vec::new();
    if let Some(start) = block_range.start {
        params.push(start);
        predicates.push(format!("first_seen_block >= ${}", params.len()));
    }
    if let Some(end) = block_range.end {
        params.push(end);
        predicates.push(format!("first_seen_block <= ${}", params.len()));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!("\n        WHERE {}", predicates.join(" AND "))
    };
    SnapshotQuery {
        sql: format!(
            "
        SELECT contract_address, token_id::text, coalesce(token_uri, ''), coalesce(image_uri, ''),
               coalesce(name, ''), coalesce(symbol, ''), coalesce({metadata_column}::text, '')
        FROM {table}{where_clause}
        ORDER BY id
        "
        ),
        params,
    }
}

fn snapshot_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new("chain", DataType::Utf8, false),
        Field::new("contract_address", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("token_uri", DataType::Utf8, false),
        Field::new("image_uri", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("metadata_json", DataType::Utf8, false),
        Field::new("token_uri_norm", DataType::Utf8, false),
        Field::new("image_uri_norm", DataType::Utf8, false),
        Field::new("name_norm", DataType::Utf8, false),
    ];
    Arc::new(Schema::new(fields))
}

fn snapshot_batch(chain: &str, rows: &[SnapshotExportRow]) -> Result<RecordBatch, AppError> {
    let schema = snapshot_schema();
    let chain = normalized_chain(chain);
    let chain_values: Vec<String> = rows.iter().map(|_| chain.clone()).collect();
    let contract_address_values: Vec<String> = rows
        .iter()
        .map(|row| canonical_contract_address(&chain, &row.contract_address))
        .collect();
    let token_id_values: Vec<String> = rows.iter().map(|row| row.token_id.clone()).collect();
    let token_uri_values: Vec<String> = rows.iter().map(|row| row.token_uri.clone()).collect();
    let image_uri_values: Vec<String> = rows.iter().map(|row| row.image_uri.clone()).collect();
    let name_values: Vec<String> = rows.iter().map(|row| row.name.clone()).collect();
    let symbol_values: Vec<String> = rows.iter().map(|row| row.symbol.clone()).collect();
    let metadata_json_values: Vec<String> =
        rows.iter().map(|row| row.metadata_json.clone()).collect();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(chain_values)),
        Arc::new(StringArray::from(contract_address_values)),
        Arc::new(StringArray::from(token_id_values)),
        Arc::new(StringArray::from(token_uri_values.clone())),
        Arc::new(StringArray::from(image_uri_values.clone())),
        Arc::new(StringArray::from(name_values.clone())),
        Arc::new(StringArray::from(symbol_values.clone())),
        Arc::new(StringArray::from(metadata_json_values)),
    ];
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
    ]);

    RecordBatch::try_new(schema, columns).map_err(|err| AppError::InvalidData(err.to_string()))
}

fn open_snapshot_writer(output_path: &Path) -> Result<ArrowWriter<std::fs::File>, AppError> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    ArrowWriter::try_new(file, snapshot_schema(), Some(props))
        .map_err(|err| AppError::InvalidData(err.to_string()))
}

pub fn write_snapshot_rows_to_parquet(
    chain: &str,
    rows: &[SnapshotExportRow],
    output_path: &Path,
) -> Result<(), AppError> {
    let mut writer = open_snapshot_writer(output_path)?;
    let batch = snapshot_batch(chain, rows)?;
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
    block_range: SnapshotBlockRange,
) -> Result<(), AppError> {
    let block_range = block_range.validate_for_chain(chain)?;
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

    let query = build_snapshot_query(&table, &metadata_column, block_range);
    let mut writer = open_snapshot_writer(output_path)?;
    let params = query
        .params
        .iter()
        .map(|value| value as &(dyn ToSql + Sync));
    let mut rows = conn.query_raw(&query.sql, params)?;
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
            let batch = snapshot_batch(chain, &buffer)?;
            writer
                .write(&batch)
                .map_err(|err| AppError::InvalidData(err.to_string()))?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        let batch = snapshot_batch(chain, &buffer)?;
        writer
            .write(&batch)
            .map_err(|err| AppError::InvalidData(err.to_string()))?;
    }
    writer
        .close()
        .map_err(|err| AppError::InvalidData(err.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_range_rejects_negative_reversed_and_solana_bounds() {
        assert!(SnapshotBlockRange::new(Some(-1), None).is_err());
        assert!(SnapshotBlockRange::new(Some(20), Some(10)).is_err());

        let range = SnapshotBlockRange::new(Some(10), Some(20)).unwrap();
        let error = range.validate_for_chain("solana").unwrap_err();
        assert!(error.to_string().contains("first_seen_block = 0"));
        assert!(range.validate_for_chain(" Solana ").is_err());
    }

    #[test]
    fn chain_table_rejects_unsafe_names() {
        assert!(chain_to_table("").is_err());
        assert!(chain_to_table("eth;drop").is_err());
    }

    #[test]
    fn snapshot_query_uses_inclusive_optional_bounds() {
        let lower = build_snapshot_query(
            "nft_assets_ethereum",
            "metadata",
            SnapshotBlockRange::new(Some(10), None).unwrap(),
        );
        assert!(lower.sql.contains("first_seen_block >= $1"));
        assert_eq!(lower.params, vec![10]);

        let upper = build_snapshot_query(
            "nft_assets_ethereum",
            "metadata",
            SnapshotBlockRange::new(None, Some(20)).unwrap(),
        );
        assert!(upper.sql.contains("first_seen_block <= $1"));
        assert_eq!(upper.params, vec![20]);

        let bounded = build_snapshot_query(
            "nft_assets_ethereum",
            "metadata",
            SnapshotBlockRange::new(Some(10), Some(20)).unwrap(),
        );
        assert!(bounded.sql.contains("first_seen_block >= $1"));
        assert!(bounded.sql.contains("first_seen_block <= $2"));
        assert_eq!(bounded.params, vec![10, 20]);
    }
}
