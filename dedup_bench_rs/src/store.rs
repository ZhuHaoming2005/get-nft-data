use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use duckdb::{params, Connection};
use serde::Serialize;
use sysinfo::System;
use top_contract_analysis_rs::analysis::scoring::metadata_document_from_json;

use crate::algorithms::{derive_name_norm, parse_keywords};
use crate::error::BenchError;
use crate::sample::BenchmarkSample;

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
    pub metadata_doc: String,
    pub metadata_keywords: Vec<String>,
}

pub struct FeatureStore {
    feature_db: PathBuf,
    feature_parquet: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResourceConfig {
    memory_limit_mb: u64,
    threads: usize,
    temp_directory: PathBuf,
}

impl FeatureStore {
    pub fn new(feature_db: impl Into<PathBuf>, feature_parquet: Option<PathBuf>) -> Self {
        Self {
            feature_db: feature_db.into(),
            feature_parquet,
        }
    }

    pub fn load_recall_rows(
        &self,
        sample: &BenchmarkSample,
    ) -> Result<(SourceInfo, Vec<FeatureRow>), BenchError> {
        let conn = open_feature_connection(&self.feature_db)?;
        ensure_feature_table(&conn)?;
        if table_exists(&conn, "nft_features")? {
            let count: i64 = conn.query_row(
                "SELECT count(*) FROM nft_features WHERE lower(chain) = lower(?)",
                params![sample.chain.as_str()],
                |row| row.get(0),
            )?;
            if count > 0 {
                return Ok((
                    SourceInfo {
                        kind: SourceKind::DuckdbTable,
                        location: self.feature_db.display().to_string(),
                    },
                    read_rows_from_table(&conn, sample)?,
                ));
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
        import_parquet_chain(&conn, sample.chain.as_str(), parquet)?;
        Ok((
            SourceInfo {
                kind: SourceKind::DuckdbTable,
                location: self.feature_db.display().to_string(),
            },
            read_rows_from_table(&conn, sample)?,
        ))
    }
}

fn open_feature_connection(feature_db: &Path) -> Result<Connection, BenchError> {
    let db_literal = feature_db.to_string_lossy();
    let use_memory = db_literal == ":memory:";
    if !use_memory {
        if let Some(parent) = feature_db.parent() {
            fs::create_dir_all(parent)?;
        }
    }
    let conn = if use_memory {
        Connection::open_in_memory()?
    } else {
        Connection::open(feature_db)?
    };
    let config = detect_resource_config(feature_db, use_memory);
    apply_resource_config(&conn, &config)?;
    Ok(conn)
}

fn detect_resource_config(feature_db: &Path, use_memory: bool) -> ResourceConfig {
    let mut system = System::new();
    system.refresh_memory();
    let total_memory_bytes = system.total_memory();
    let gib = 1024_u64 * 1024 * 1024;
    let min_limit = 512_u64 * 1024 * 1024;
    let soft_cap = 8_u64 * gib;
    let reserved = if total_memory_bytes >= 8 * gib {
        gib
    } else {
        512_u64 * 1024 * 1024
    };
    let proportional = total_memory_bytes.saturating_mul(35) / 100;
    let available_target = total_memory_bytes.saturating_sub(reserved);
    let selected = proportional.min(available_target).max(min_limit).min(soft_cap);
    let memory_limit_mb = (selected / (1024 * 1024)).max(512);
    let threads = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(1, 8);
    let temp_directory = if use_memory {
        std::env::temp_dir().join("dedup_bench_rs_duckdb_tmp")
    } else {
        feature_db
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".duckdb_tmp")
    };
    ResourceConfig {
        memory_limit_mb,
        threads,
        temp_directory,
    }
}

fn apply_resource_config(conn: &Connection, config: &ResourceConfig) -> Result<(), BenchError> {
    fs::create_dir_all(&config.temp_directory)?;
    let temp_dir = config
        .temp_directory
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    conn.execute_batch(&format!(
        "
        SET memory_limit='{}MB';
        SET temp_directory='{}';
        SET threads={};
        ",
        config.memory_limit_mb, temp_dir, config.threads
    ))?;
    Ok(())
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, BenchError> {
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        params![table_name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn ensure_feature_table(conn: &Connection) -> Result<(), BenchError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS nft_features (
            chain VARCHAR,
            contract_address VARCHAR,
            token_id VARCHAR,
            token_uri VARCHAR,
            image_uri VARCHAR,
            name VARCHAR,
            symbol VARCHAR,
            metadata_json VARCHAR,
            token_uri_norm VARCHAR,
            image_uri_norm VARCHAR,
            name_norm VARCHAR,
            symbol_norm VARCHAR,
            metadata_doc VARCHAR,
            metadata_keywords_arr VARCHAR
        );
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS token_uri VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS image_uri VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS symbol VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS metadata_json VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS token_uri_norm VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS image_uri_norm VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS name_norm VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS symbol_norm VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS metadata_doc VARCHAR;
        ALTER TABLE nft_features ADD COLUMN IF NOT EXISTS metadata_keywords_arr VARCHAR;
        ",
    )?;
    Ok(())
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

fn sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn build_table_recall_predicate(sample: &BenchmarkSample, columns: &[String]) -> Option<String> {
    let has_column = |name: &str| columns.iter().any(|column| column == name);
    let mut clauses = Vec::new();

    if let Some(prefix) = sample.name_prefix() {
        if has_column("name_norm") {
            let escaped_prefix = sql_string_literal(&prefix.to_lowercase());
            clauses.push(format!(
                "coalesce(lower(cast(name_norm as varchar)), '') LIKE '{escaped_prefix}%'"
            ));
            if has_column("name") {
                clauses.push(
                    "(coalesce(cast(name_norm as varchar), '') = '' AND coalesce(cast(name as varchar), '') <> '')"
                        .to_string(),
                );
            }
        } else if has_column("name") {
            clauses.push("coalesce(cast(name as varchar), '') <> ''".to_string());
        }
    }

    if !sample.metadata_keywords.is_empty() {
        if has_column("metadata_doc") {
            let keyword_predicate = sample
                .metadata_keywords
                .iter()
                .map(|keyword| {
                    let escaped_keyword = sql_string_literal(&keyword.to_lowercase());
                    format!(
                        "regexp_matches(lower(cast(metadata_doc as varchar)), '(^|[^[:alnum:]_]){escaped_keyword}([^[:alnum:]_]|$)')"
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            if !keyword_predicate.is_empty() {
                clauses.push(format!("({keyword_predicate})"));
            }
        }
        if has_column("metadata_json") {
            let missing_doc = if has_column("metadata_doc") {
                "coalesce(cast(metadata_doc as varchar), '') = ''"
            } else {
                "TRUE"
            };
            let missing_keywords = if has_column("metadata_keywords_arr") {
                "coalesce(cast(metadata_keywords_arr as varchar), '') = ''"
            } else {
                "TRUE"
            };
            clauses.push(format!(
                "(({missing_doc}) OR ({missing_keywords})) AND coalesce(cast(metadata_json as varchar), '') <> ''"
            ));
        }
    }

    if clauses.is_empty() {
        None
    } else {
        Some(format!("({})", clauses.join(" OR ")))
    }
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

fn import_parquet_chain(
    conn: &Connection,
    chain: &str,
    parquet_path: &Path,
) -> Result<(), BenchError> {
    let columns = parquet_columns(conn, parquet_path)?;
    let path_literal = parquet_path.to_string_lossy().replace('\\', "/");
    let chain_literal = chain.replace('\'', "''");
    let token_uri_expr = if columns.iter().any(|column| column == "token_uri") {
        "coalesce(cast(token_uri as varchar), '')"
    } else {
        "''"
    };
    let image_uri_expr = if columns.iter().any(|column| column == "image_uri") {
        "coalesce(cast(image_uri as varchar), '')"
    } else {
        "''"
    };
    let symbol_expr = if columns.iter().any(|column| column == "symbol") {
        "coalesce(cast(symbol as varchar), '')"
    } else {
        "''"
    };
    let metadata_json_expr = if columns.iter().any(|column| column == "metadata_json") {
        "coalesce(cast(metadata_json as varchar), '')"
    } else {
        "''"
    };
    let token_uri_norm_expr = if columns.iter().any(|column| column == "token_uri_norm") {
        "coalesce(cast(token_uri_norm as varchar), '')"
    } else {
        "''"
    };
    let image_uri_norm_expr = if columns.iter().any(|column| column == "image_uri_norm") {
        "coalesce(cast(image_uri_norm as varchar), '')"
    } else {
        "''"
    };
    let name_norm_expr = if columns.iter().any(|column| column == "name_norm") {
        "coalesce(cast(name_norm as varchar), '')"
    } else {
        "''"
    };
    let symbol_norm_expr = if columns.iter().any(|column| column == "symbol_norm") {
        "coalesce(cast(symbol_norm as varchar), '')"
    } else {
        "''"
    };
    let metadata_doc_expr = if columns.iter().any(|column| column == "metadata_doc") {
        "coalesce(cast(metadata_doc as varchar), '')"
    } else {
        "''"
    };
    let metadata_keywords_expr = if columns.iter().any(|column| column == "metadata_keywords_arr") {
        "coalesce(cast(metadata_keywords_arr as varchar), '')"
    } else {
        "''"
    };
    let source_chain_expr = if columns.iter().any(|column| column == "chain") {
        "lower(cast(chain as varchar))"
    } else {
        &format!("'{chain_literal}'")
    };
    let where_clause = if columns.iter().any(|column| column == "chain") {
        format!("WHERE lower(cast(chain as varchar)) = '{chain_literal}'")
    } else {
        String::new()
    };
    conn.execute("DELETE FROM nft_features WHERE lower(chain) = lower(?)", params![chain])?;
    let insert_sql = format!(
        "
        INSERT INTO nft_features (
            chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
            token_uri_norm, image_uri_norm, name_norm, symbol_norm, metadata_doc, metadata_keywords_arr
        )
        SELECT
            {source_chain_expr} AS chain,
            lower(cast(contract_address as varchar)) AS contract_address,
            cast(token_id as varchar) AS token_id,
            {token_uri_expr} AS token_uri,
            {image_uri_expr} AS image_uri,
            coalesce(cast(name as varchar), '') AS name,
            {symbol_expr} AS symbol,
            {metadata_json_expr} AS metadata_json,
            {token_uri_norm_expr} AS token_uri_norm,
            {image_uri_norm_expr} AS image_uri_norm,
            {name_norm_expr} AS name_norm,
            {symbol_norm_expr} AS symbol_norm,
            {metadata_doc_expr} AS metadata_doc,
            {metadata_keywords_expr} AS metadata_keywords_arr
        FROM read_parquet('{path_literal}')
        {where_clause}
        "
    );
    conn.execute_batch(&insert_sql)?;
    Ok(())
}

fn read_rows_from_table(
    conn: &Connection,
    sample: &BenchmarkSample,
) -> Result<Vec<FeatureRow>, BenchError> {
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
    let recall_predicate = build_table_recall_predicate(sample, &columns)
        .map(|predicate| format!(" AND {predicate}"))
        .unwrap_or_default();
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
        {recall_predicate}
        "
    );
    collect_recall_rows_from_query(conn, &query, params![sample.chain.as_str()], sample)
}

fn collect_recall_rows_from_query<P>(
    conn: &Connection,
    query: &str,
    params: P,
    sample: &BenchmarkSample,
) -> Result<Vec<FeatureRow>, BenchError>
where
    P: duckdb::Params,
{
    let sample_name_prefix = sample.name_prefix();
    let sample_keyword_set: HashSet<&str> =
        sample.metadata_keywords.iter().map(String::as_str).collect();
    let sample_has_contract = !sample.contract_address.is_empty();

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
        let (
            contract_address,
            token_id,
            name,
            metadata_json,
            raw_metadata_doc,
            raw_name_norm,
            keywords_raw,
        ) = row?;

        if sample_has_contract
            && sample.contract_address.eq_ignore_ascii_case(&contract_address)
        {
            continue;
        }

        let mut name_norm = raw_name_norm;
        let name_match = if let Some(prefix) = sample_name_prefix.as_ref() {
            if name_norm.trim().is_empty() {
                name_norm = derive_name_norm(&name);
            }
            !name_norm.is_empty() && name_norm.starts_with(prefix)
        } else {
            false
        };

        let mut metadata_doc = raw_metadata_doc;
        let mut metadata_keywords = None;
        let metadata_match = if sample_keyword_set.is_empty() {
            false
        } else {
            if metadata_doc.trim().is_empty() && keywords_raw.trim().is_empty() {
                metadata_doc = metadata_document_from_json(&metadata_json);
            }
            let keywords = parse_keywords(&keywords_raw, &metadata_doc);
            let matched = keywords
                .iter()
                .any(|keyword| sample_keyword_set.contains(keyword.as_str()));
            metadata_keywords = Some(keywords);
            matched
        };

        if !name_match && !metadata_match {
            continue;
        }

        if metadata_doc.trim().is_empty() {
            metadata_doc = metadata_document_from_json(&metadata_json);
        }
        let metadata_keywords =
            metadata_keywords.unwrap_or_else(|| parse_keywords(&keywords_raw, &metadata_doc));
        if name_norm.trim().is_empty() {
            name_norm = derive_name_norm(&name);
        }

        output.push(FeatureRow {
            contract_address,
            token_id,
            name,
            name_norm,
            metadata_doc,
            metadata_keywords,
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
        let sample = BenchmarkSample {
            chain: "ethereum".into(),
            contract_address: String::new(),
            token_id: String::new(),
            name: "Azuki #1".into(),
            name_norm: derive_name_norm("Azuki #1"),
            metadata_json: "{\"description\":\"rare dragon gold\"}".into(),
            metadata_doc: "rare dragon gold".into(),
            metadata_keywords: vec!["rare".into(), "dragon".into(), "gold".into()],
        };
        let (_, rows) = store.load_recall_rows(&sample).unwrap();
        assert_eq!(rows[0].metadata_doc, "rare dragon gold");
        assert!(rows[0].metadata_keywords.contains(&"dragon".to_string()));
    }

    #[test]
    fn parquet_fallback_creates_disk_duckdb_file_when_path_is_missing() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("feature_store.duckdb");
        let parquet_path = dir.path().join("snapshot.parquet");
        let conn = Connection::open_in_memory().unwrap();
        let path = parquet_path.to_string_lossy().replace('\\', "/");
        conn.execute_batch(&format!(
            "COPY (
                SELECT
                    'ethereum' AS chain,
                    '0xdup' AS contract_address,
                    '1' AS token_id,
                    'Azuki Mirror #1' AS name,
                    '{{\"description\":\"rare dragon gold\"}}' AS metadata_json
            ) TO '{path}' (FORMAT PARQUET)"
        ))
        .unwrap();

        let store = FeatureStore::new(db_path.clone(), Some(parquet_path));
        let sample = BenchmarkSample {
            chain: "ethereum".into(),
            contract_address: String::new(),
            token_id: String::new(),
            name: "Azuki #1".into(),
            name_norm: derive_name_norm("Azuki #1"),
            metadata_json: "{\"description\":\"rare dragon gold\"}".into(),
            metadata_doc: "rare dragon gold".into(),
            metadata_keywords: vec!["rare".into(), "dragon".into(), "gold".into()],
        };
        let (_, rows) = store.load_recall_rows(&sample).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(db_path.exists());

        let persisted = Connection::open(&db_path).unwrap();
        let persisted_count: i64 = persisted
            .query_row(
                "SELECT count(*) FROM nft_features WHERE lower(chain) = 'ethereum'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(persisted_count, 1);
    }

    #[test]
    fn detect_resource_config_reserves_headroom() {
        let config = detect_resource_config(Path::new("feature_store.duckdb"), false);
        assert!(config.memory_limit_mb >= 512);
        assert!((1..=8).contains(&config.threads));
        assert!(config.temp_directory.ends_with(".duckdb_tmp"));
    }

    #[test]
    fn table_recall_predicate_pushes_down_name_and_metadata_filters() {
        let sample = BenchmarkSample {
            chain: "ethereum".into(),
            contract_address: String::new(),
            token_id: String::new(),
            name: "Azuki #1".into(),
            name_norm: derive_name_norm("Azuki #1"),
            metadata_json: "{\"description\":\"rare dragon gold\"}".into(),
            metadata_doc: "rare dragon gold".into(),
            metadata_keywords: vec!["rare".into(), "dragon".into(), "gold".into()],
        };
        let columns = vec![
            "chain".to_string(),
            "name".to_string(),
            "name_norm".to_string(),
            "metadata_json".to_string(),
            "metadata_doc".to_string(),
            "metadata_keywords_arr".to_string(),
        ];

        let predicate = build_table_recall_predicate(&sample, &columns).unwrap();

        assert!(predicate.contains("name_norm"));
        assert!(predicate.contains("regexp_matches"));
        assert!(predicate.contains("metadata_json"));
    }

    #[test]
    fn excludes_all_rows_from_sample_contract() {
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
                metadata_json VARCHAR,
                metadata_doc VARCHAR,
                name_norm VARCHAR,
                metadata_keywords_arr VARCHAR
            );
            INSERT INTO nft_features VALUES
            ('ethereum', '0xseed', '2', 'Azuki #2', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'azuki', '[\"rare\",\"dragon\",\"gold\"]'),
            ('ethereum', '0xdup', '3', 'Azuki #3', '{\"description\":\"rare dragon gold\"}', 'rare dragon gold', 'azuki', '[\"rare\",\"dragon\",\"gold\"]');
            ",
        )
        .unwrap();
        drop(conn);

        let store = FeatureStore::new(db_path, None);
        let sample = BenchmarkSample {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            name: "Azuki #1".into(),
            name_norm: derive_name_norm("Azuki #1"),
            metadata_json: "{\"description\":\"rare dragon gold\"}".into(),
            metadata_doc: "rare dragon gold".into(),
            metadata_keywords: vec!["rare".into(), "dragon".into(), "gold".into()],
        };

        let (_, rows) = store.load_recall_rows(&sample).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].contract_address, "0xdup");
    }
}
