use std::path::{Path, PathBuf};

use duckdb::{params, Connection};
use serde::Serialize;
use top_contract_analysis_rs::analysis::scoring::metadata_document_from_json;

use crate::algorithms::{derive_name_norm, parse_keywords};
use crate::error::BenchError;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    DuckdbTable,
    ParquetFile,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct SourceInfo {
    pub kind: SourceKind,
    pub location: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureRow {
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub name_norm: String,
    pub metadata_json: String,
    pub metadata_doc: String,
    pub metadata_keywords: Vec<String>,
}

pub struct FeatureStore {
    feature_db: PathBuf,
    feature_parquet: Option<PathBuf>,
}

impl FeatureStore {
    pub fn new(feature_db: impl Into<PathBuf>, feature_parquet: Option<PathBuf>) -> Self {
        Self {
            feature_db: feature_db.into(),
            feature_parquet,
        }
    }

    pub fn load_rows(&self, chain: &str) -> Result<(SourceInfo, Vec<FeatureRow>), BenchError> {
        let feature_db_literal = self.feature_db.to_string_lossy();
        if feature_db_literal == ":memory:" || self.feature_db.exists() {
            let conn = if feature_db_literal == ":memory:" {
                Connection::open_in_memory()?
            } else {
                Connection::open(&self.feature_db)?
            };
            if table_exists(&conn, "nft_features")? {
                let count: i64 = conn.query_row(
                    "SELECT count(*) FROM nft_features WHERE lower(chain) = lower(?)",
                    params![chain],
                    |row| row.get(0),
                )?;
                if count > 0 {
                    return Ok((
                        SourceInfo {
                            kind: SourceKind::DuckdbTable,
                            location: self.feature_db.display().to_string(),
                        },
                        read_rows_from_table(&conn, chain)?,
                    ));
                }
            }
        }

        let parquet = self.feature_parquet.as_ref().ok_or_else(|| {
            BenchError::InvalidData(format!(
                "no usable chain data found in feature db {:?} and no parquet fallback provided",
                self.feature_db
            ))
        })?;
        if !parquet.exists() {
            return Err(BenchError::InvalidData(format!(
                "feature parquet not found: {}",
                parquet.display()
            )));
        }
        let conn = Connection::open_in_memory()?;
        Ok((
            SourceInfo {
                kind: SourceKind::ParquetFile,
                location: parquet.display().to_string(),
            },
            read_rows_from_parquet(&conn, chain, parquet)?,
        ))
    }
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, BenchError> {
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        params![table_name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn table_columns(conn: &Connection, table_name: &str) -> Result<Vec<String>, BenchError> {
    let mut stmt = conn.prepare(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
    )?;
    let rows = stmt.query_map(params![table_name], |row| row.get::<_, String>(0))?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn parquet_columns(conn: &Connection, parquet_path: &Path) -> Result<Vec<String>, BenchError> {
    let query = format!(
        "DESCRIBE SELECT * FROM read_parquet('{}')",
        parquet_path.to_string_lossy().replace('\\', "/")
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn read_rows_from_table(conn: &Connection, chain: &str) -> Result<Vec<FeatureRow>, BenchError> {
    let columns = table_columns(conn, "nft_features")?;
    let metadata_json_expr = if columns.iter().any(|column| column == "metadata_json") {
        "coalesce(cast(metadata_json as varchar), '')"
    } else {
        "''"
    };
    let metadata_doc_expr = if columns.iter().any(|column| column == "metadata_doc") {
        "coalesce(cast(metadata_doc as varchar), '')"
    } else {
        "''"
    };
    let name_norm_expr = if columns.iter().any(|column| column == "name_norm") {
        "coalesce(cast(name_norm as varchar), '')"
    } else {
        "''"
    };
    let keywords_expr = if columns.iter().any(|column| column == "metadata_keywords_arr") {
        "coalesce(cast(metadata_keywords_arr as varchar), '')"
    } else {
        "''"
    };
    let query = format!(
        "
        SELECT
            lower(cast(contract_address as varchar)) AS contract_address,
            cast(token_id as varchar) AS token_id,
            coalesce(cast(name as varchar), '') AS name,
            {metadata_json_expr} AS metadata_json,
            {metadata_doc_expr} AS metadata_doc,
            {name_norm_expr} AS name_norm,
            {keywords_expr} AS metadata_keywords_raw
        FROM nft_features
        WHERE lower(chain) = lower(?)
        ORDER BY contract_address, token_id
        "
    );
    read_rows_from_query(conn, &query, params![chain])
}

fn read_rows_from_parquet(
    conn: &Connection,
    chain: &str,
    parquet_path: &Path,
) -> Result<Vec<FeatureRow>, BenchError> {
    let columns = parquet_columns(conn, parquet_path)?;
    let path_literal = parquet_path.to_string_lossy().replace('\\', "/");
    let metadata_json_expr = if columns.iter().any(|column| column == "metadata_json") {
        "coalesce(cast(metadata_json as varchar), '')"
    } else {
        "''"
    };
    let metadata_doc_expr = if columns.iter().any(|column| column == "metadata_doc") {
        "coalesce(cast(metadata_doc as varchar), '')"
    } else {
        "''"
    };
    let name_norm_expr = if columns.iter().any(|column| column == "name_norm") {
        "coalesce(cast(name_norm as varchar), '')"
    } else {
        "''"
    };
    let keywords_expr = if columns.iter().any(|column| column == "metadata_keywords_arr") {
        "coalesce(cast(metadata_keywords_arr as varchar), '')"
    } else {
        "''"
    };
    let where_clause = if columns.iter().any(|column| column == "chain") {
        "WHERE lower(cast(chain as varchar)) = lower(?)"
    } else {
        ""
    };
    let query = format!(
        "
        SELECT
            lower(cast(contract_address as varchar)) AS contract_address,
            cast(token_id as varchar) AS token_id,
            coalesce(cast(name as varchar), '') AS name,
            {metadata_json_expr} AS metadata_json,
            {metadata_doc_expr} AS metadata_doc,
            {name_norm_expr} AS name_norm,
            {keywords_expr} AS metadata_keywords_raw
        FROM read_parquet('{path_literal}')
        {where_clause}
        ORDER BY contract_address, token_id
        "
    );
    if where_clause.is_empty() {
        read_rows_from_query(conn, &query, params![])
    } else {
        read_rows_from_query(conn, &query, params![chain])
    }
}

fn read_rows_from_query<P>(
    conn: &Connection,
    query: &str,
    params: P,
) -> Result<Vec<FeatureRow>, BenchError>
where
    P: duckdb::Params,
{
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map(params, |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
        ))
    })?;
    let mut output = Vec::new();
    for row in rows {
        let (contract_address, token_id, name, metadata_json, metadata_doc, name_norm, keywords_raw) =
            row?;
        let metadata_doc = if metadata_doc.trim().is_empty() {
            metadata_document_from_json(&metadata_json)
        } else {
            metadata_doc
        };
        output.push(FeatureRow {
            contract_address,
            token_id,
            name: name.clone(),
            name_norm: if name_norm.trim().is_empty() {
                derive_name_norm(&name)
            } else {
                name_norm
            },
            metadata_json,
            metadata_doc: metadata_doc.clone(),
            metadata_keywords: parse_keywords(&keywords_raw, &metadata_doc),
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_keyword_values_from_json_and_duckdb_style_lists() {
        assert_eq!(
            crate::algorithms::parse_keywords("[\"dragon\",\"gold\"]", "rare dragon gold"),
            vec!["dragon".to_string(), "gold".to_string()]
        );
        assert_eq!(
            crate::algorithms::parse_keywords("[dragon, gold]", "rare dragon gold"),
            vec!["dragon".to_string(), "gold".to_string()]
        );
    }

    #[test]
    fn falls_back_to_computed_metadata_doc_and_keywords() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("bench.duckdb");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE nft_features (
                chain VARCHAR,
                contract_address VARCHAR,
                token_id VARCHAR,
                name VARCHAR,
                metadata_json VARCHAR
            );
            INSERT INTO nft_features VALUES
            ('ethereum', '0xdup', '1', 'Azuki Mirror #1', '{\"description\":\"rare dragon gold\"}');
            ",
        )
        .unwrap();
        drop(conn);

        let store = FeatureStore::new(db_path, None);
        let (_, rows) = store.load_rows("ethereum").unwrap();
        assert_eq!(rows[0].metadata_doc, "rare dragon gold");
        assert!(rows[0].metadata_keywords.contains(&"dragon".to_string()));
    }
}
