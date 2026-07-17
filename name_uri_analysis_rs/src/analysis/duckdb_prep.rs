use super::*;

use std::io::Write;

use crate::replace_file_atomically;

static DUCKDB_PROFILE_SEQUENCE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[derive(Serialize)]
struct DuckDbProfileNode {
    metrics: HashMap<String, String>,
    children: Vec<DuckDbProfileNode>,
}

impl From<duckdb::profiling::ProfilingInfo> for DuckDbProfileNode {
    fn from(value: duckdb::profiling::ProfilingInfo) -> Self {
        Self {
            metrics: value.metrics,
            children: value.children.into_iter().map(Self::from).collect(),
        }
    }
}

pub(crate) fn enable_prepare_profiling(
    conn: &Connection,
    directory: &Path,
) -> Result<(), AnalysisError> {
    fs::create_dir_all(directory)?;
    std::env::set_var("NAME_URI_DUCKDB_PROFILE_DIR", directory);
    conn.execute_batch(
        "PRAGMA enable_profiling='no_output';
         PRAGMA profiling_mode='detailed';",
    )?;
    Ok(())
}

fn persist_last_duckdb_profile(conn: &Connection, label: &str) -> Result<(), AnalysisError> {
    let Some(directory) = std::env::var_os("NAME_URI_DUCKDB_PROFILE_DIR") else {
        return Ok(());
    };
    let Some(profile) = conn.get_profiling_info() else {
        return Ok(());
    };
    let sequence = DUCKDB_PROFILE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let slug = label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let destination = PathBuf::from(directory).join(format!("{sequence:02}-{slug}.json"));
    let partial = destination.with_extension("json.partial");
    let mut file = fs::File::create(&partial)?;
    serde_json::to_writer_pretty(&mut file, &DuckDbProfileNode::from(profile))?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&partial, &destination)?;
    Ok(())
}

pub(crate) fn configure_duckdb(
    conn: &Connection,
    options: &AnalysisOptions,
) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "
        PRAGMA preserve_insertion_order=false;
        SET parquet_metadata_cache=true;
        ",
    )?;
    let duckdb_threads = std::env::var("NAME_URI_ANALYSIS_DUCKDB_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or_else(|| options.threads.clamp(1, DUCKDB_THREAD_CAP))
        .min(options.threads.max(1));
    conn.execute(&format!("PRAGMA threads={duckdb_threads}"), [])?;
    let memory_limit = resolve_duckdb_memory_limit(&options.duckdb_memory_limit)?;
    conn.execute(
        &format!("PRAGMA memory_limit='{}'", sql_string(&memory_limit)),
        [],
    )?;
    if let Some(temp_directory) = &options.temp_directory {
        fs::create_dir_all(temp_directory)?;
        conn.execute(
            &format!(
                "PRAGMA temp_directory='{}'",
                sql_string(&temp_directory.display().to_string().replace('\\', "/"))
            ),
            [],
        )?;
    }
    Ok(())
}

/// Resolve the DuckDB `memory_limit` PRAGMA value. `auto` derives ~75% of
/// available memory so DuckDB and the Rust analysis structures can coexist in
/// one process; any other value is validated as a byte size and passed through.
pub(crate) fn resolve_duckdb_memory_limit(value: &str) -> Result<String, AnalysisError> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("auto") {
        let available = usize::try_from(effective_available_memory_bytes()).unwrap_or(usize::MAX);
        let duckdb = available.saturating_mul(3).saturating_div(4);
        let mib = 1024usize * 1024;
        Ok(format!("{}MB", duckdb / mib))
    } else {
        parse_byte_size(trimmed)?;
        Ok(trimmed.to_string())
    }
}

pub(crate) fn prepare_base_tables(
    conn: &Connection,
    options: &AnalysisOptions,
    progress: &ProgressTracker,
) -> Result<Vec<String>, AnalysisError> {
    let inputs = parquet_input_sql(&options.parquet_inputs);
    let input_columns = parquet_input_columns(conn, &inputs)?;
    let metadata_json_expr = metadata_json_projection_expr(&input_columns);
    let source_file_relation = source_file_id_relation_sql(&options.parquet_inputs);
    progress.start_stage("preparing DuckDB tables", 11);
    execute_progress_batch(
        conn,
        &build_core_rows_sql(&inputs),
        progress,
        "materialized contract/name projection",
    )?;
    execute_progress_batch(
        conn,
        "
            CREATE OR REPLACE TABLE selected_chains AS
            WITH chains AS (
                SELECT DISTINCT lower(trim(CAST(chain AS VARCHAR))) AS chain
                FROM contract_dim
                WHERE chain <> ''
            )
            SELECT chain,
                   (row_number() OVER (ORDER BY chain) - 1)::UINTEGER AS chain_index
            FROM chains;
        ",
        progress,
        "loaded selected chains",
    )?;
    let chains = load_selected_chains(conn)?;
    let include_cross_chain = chains.len() > 1;
    if include_cross_chain {
        progress.add_work(3);
    }
    materialize_uri_metadata_projection(
        conn,
        &inputs,
        &metadata_json_expr,
        &source_file_relation,
        progress,
    )?;
    execute_progress_batch(
        conn,
        &analysis_contracts_sql(),
        progress,
        "materialized contract statistics",
    )?;
    execute_progress_batch(
        conn,
        build_chain_totals_sql(),
        progress,
        "built compact chain totals",
    )?;
    super::metadata::prepare_metadata_compact_tables(conn, progress)?;
    execute_progress_batch(
        conn,
        "
            CREATE OR REPLACE TABLE name_atoms AS
            WITH atoms AS (
                SELECT chain,
                       name_norm,
                       count(*)::BIGINT AS contract_count,
                       coalesce(sum(nft_count), 0)::BIGINT AS nft_count
                FROM analysis_contracts
                WHERE name_norm IS NOT NULL
                GROUP BY chain, name_norm
            )
            SELECT *
            FROM atoms
            WHERE name_norm <> '';
        ",
        progress,
        "built name atoms",
    )?;
    build_uri_key_stats(conn, progress, include_cross_chain)?;
    build_uri_contract_flags(conn, progress, include_cross_chain)?;
    progress.finish_stage("DuckDB tables ready");
    Ok(chains)
}

pub(super) fn analysis_contracts_sql() -> String {
    "
        CREATE OR REPLACE TEMP TABLE metadata_contract_token_sources AS
        SELECT contract_id,
               token_id,
               arg_min(
                   struct_pack(
                       file_id := source_file,
                       row_number := source_row_number
                   ),
                   row(source_file, source_row_number)
               ) AS metadata_source,
               max(metadata_json_bytes)::UBIGINT AS metadata_max_json_bytes
        FROM metadata_rows
        WHERE metadata_eligible
        GROUP BY contract_id, token_id;

        CREATE OR REPLACE TABLE analysis_contracts AS
        WITH metadata_sources AS (
            SELECT contract_id,
                   arg_min(
                       metadata_source,
                       row(
                           token_id,
                           metadata_source.file_id,
                           metadata_source.row_number
                       )
                   ) AS metadata_source,
                   max(metadata_max_json_bytes)::UBIGINT AS metadata_max_json_bytes
            FROM metadata_contract_token_sources
            GROUP BY contract_id
        ),
        indexed_metadata_sources AS (
            SELECT contract_id,
                   metadata_source.file_id AS metadata_source_file,
                   metadata_source.row_number AS metadata_source_row_number,
                   metadata_max_json_bytes,
                   -- Dense IDs need only be unique within this Prepare run.
                   -- Global sort-by-contract_id is intentionally omitted so
                   -- assignment can follow unordered hash-aggregate output.
                   row_number() OVER () - 1 AS metadata_contract_index
            FROM metadata_sources
        )
        SELECT contracts.contract_id,
               contracts.chain,
               contracts.contract_address,
               contracts.nft_count,
               contracts.name_norm,
               metadata.metadata_source_file,
               metadata.metadata_source_row_number,
               metadata.metadata_contract_index,
               metadata.metadata_max_json_bytes
        FROM contract_dim contracts
        LEFT JOIN indexed_metadata_sources metadata
          ON metadata.contract_id = contracts.contract_id
        ;
        "
    .to_string()
}

#[cfg(test)]
pub(crate) fn metadata_json_eligible_predicate(column: &str) -> String {
    // Keep in sync with top_contract_analysis_rs::DuckDbFeatureStore::sql_metadata_json_eligible_predicate
    // and metadata_is_dedup_eligible: trim, non-empty, len<=64KiB, starts with { or [
    let trimmed = format!("trim(coalesce(CAST({column} AS VARCHAR), ''))");
    format!(
        "{trimmed} <> ''
         AND octet_length(encode({trimmed})) <= {MAX_METADATA_BYTES_FOR_DEDUP}
         AND (
             starts_with({trimmed}, '{{')
             OR starts_with({trimmed}, '[')
         )"
    )
}

fn normalized_metadata_json_eligible_predicate(column: &str, byte_length_column: &str) -> String {
    format!(
        "{column} <> ''
         AND {byte_length_column} <= {MAX_METADATA_BYTES_FOR_DEDUP}
         AND (starts_with({column}, '{{') OR starts_with({column}, '['))"
    )
}

pub(crate) fn build_core_rows_sql(inputs: &str) -> String {
    format!(
        "
        CREATE OR REPLACE TEMP TABLE contract_dim AS
        WITH raw AS (
             SELECT lower(trim(CAST(chain AS VARCHAR))) AS chain,
                    trim(CAST(contract_address AS VARCHAR)) AS contract_address,
                    trim(coalesce(CAST(name_norm AS VARCHAR), '')) AS name_norm
             FROM read_parquet({inputs})
             WHERE chain IS NOT NULL
        ), normalized AS (
             SELECT chain,
                    CASE
                        WHEN chain = 'solana' THEN contract_address
                        ELSE lower(contract_address)
                    END AS contract_address,
                    name_norm
             FROM raw
             WHERE chain <> ''
        ), aggregated AS (
            SELECT chain,
                   contract_address,
                   count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm
            FROM normalized
            WHERE contract_address <> ''
            GROUP BY chain, contract_address
        )
        SELECT (row_number() OVER () - 1)::UINTEGER AS contract_id,
               *
        FROM aggregated;
        "
    )
}

pub(crate) fn source_file_id_relation_sql(paths: &[PathBuf]) -> String {
    let rows = paths
        .iter()
        .enumerate()
        .map(|(file_id, path)| {
            format!(
                "('{}', {}::UINTEGER)",
                sql_string(&path.display().to_string().replace('\\', "/")),
                file_id
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!("(VALUES\n{rows}\n) AS source_files(filename, file_id)")
}

pub(crate) fn build_uri_metadata_projection_sql(
    inputs: &str,
    metadata_json_expr: &str,
    source_file_relation: &str,
) -> String {
    format!(
        "CREATE OR REPLACE TABLE prepare_uri_metadata_rows AS
         WITH raw AS (
             SELECT lower(trim(CAST(chain AS VARCHAR))) AS chain,
                    trim(CAST(contract_address AS VARCHAR)) AS contract_address,
                    coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                    coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                    trim(coalesce(CAST(token_id AS VARCHAR), '')) AS token_id,
                    {metadata_json_expr} AS metadata_json,
                    replace(CAST(filename AS VARCHAR), chr(92), '/') AS source_filename,
                    file_row_number::UBIGINT AS source_row_number
             FROM read_parquet({inputs}, filename = true, file_row_number = true)
             WHERE chain IS NOT NULL
         ),
         projected AS (
             SELECT chain,
                    CASE
                        WHEN chain = 'solana' THEN contract_address
                        ELSE lower(contract_address)
                    END AS contract_address,
                    token_uri_norm,
                    image_uri_norm,
                    token_id,
                    metadata_json,
                    source_filename,
                    source_row_number
             FROM raw
             WHERE chain <> ''
         ),
         sized AS (
             SELECT projected.*,
                    least(
                        octet_length(encode(projected.metadata_json)),
                        {ineligible_length_sentinel}
                    )::UINTEGER AS metadata_json_bytes
             FROM projected
         ),
         classified AS (
             SELECT sized.*,
                    ({metadata_eligible}) AS metadata_eligible
             FROM sized
         )
         SELECT chains.chain_index::UINTEGER AS chain_index,
                contracts.contract_id::UINTEGER AS contract_id,
                classified.token_uri_norm,
                classified.image_uri_norm,
                classified.token_id,
                CASE
                    WHEN classified.metadata_eligible THEN classified.metadata_json
                    ELSE ''
                END AS metadata_json,
                CASE
                    WHEN classified.metadata_eligible THEN classified.metadata_json_bytes
                    ELSE 0::UINTEGER
                END AS metadata_json_bytes,
                coalesce(
                    source_files.file_id,
                    error('Parquet filename is absent from the stable file_id map')::UINTEGER
                ) AS source_file,
                classified.source_row_number,
                classified.metadata_eligible
         FROM classified
         LEFT JOIN {source_file_relation}
           ON source_files.filename = classified.source_filename
         JOIN contract_dim contracts
           ON contracts.chain = classified.chain
          AND contracts.contract_address = classified.contract_address
         JOIN selected_chains chains ON chains.chain = contracts.chain
         WHERE classified.token_uri_norm <> ''
            OR classified.image_uri_norm <> ''
            OR classified.metadata_eligible",
        metadata_eligible = normalized_metadata_json_eligible_predicate(
            "sized.metadata_json",
            "sized.metadata_json_bytes"
        ),
        ineligible_length_sentinel = MAX_METADATA_BYTES_FOR_DEDUP + 1,
    )
}

pub(crate) fn build_uri_metadata_views_sql() -> &'static str {
    "
        CREATE OR REPLACE TEMP VIEW uri_rows AS
        SELECT chain_index,
               contract_id,
               token_uri_norm,
               image_uri_norm
        FROM prepare_uri_metadata_rows
        WHERE token_uri_norm <> '' OR image_uri_norm <> '';

        CREATE OR REPLACE TEMP VIEW metadata_rows AS
        SELECT contract_id,
               token_id,
               metadata_json,
               metadata_json_bytes,
               source_file,
               source_row_number,
               metadata_eligible
        FROM prepare_uri_metadata_rows
        WHERE metadata_eligible;
    "
}

pub(crate) fn materialize_uri_metadata_projection(
    conn: &Connection,
    inputs: &str,
    metadata_json_expr: &str,
    source_file_relation: &str,
    progress: &ProgressTracker,
) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "DROP TABLE IF EXISTS metadata_rows;
         DROP TABLE IF EXISTS prepare_uri_metadata_rows;
         DROP TABLE IF EXISTS metadata_rows_materialized;",
    )?;
    progress.start_task("materializing URI + metadata projection", None, "rows");
    conn.execute_batch(&build_uri_metadata_projection_sql(
        inputs,
        metadata_json_expr,
        source_file_relation,
    ))?;
    persist_last_duckdb_profile(conn, "materialized URI + metadata projection")?;
    conn.execute_batch(build_uri_metadata_views_sql())?;
    progress.finish_task("URI + metadata projection ready");
    progress.step_stage("materialized URI projection");
    progress.step_stage("materialized eligible metadata projection");
    Ok(())
}

pub(crate) fn materialize_metadata_rows_after_uri(conn: &Connection) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "CREATE OR REPLACE TABLE metadata_rows_materialized AS
         SELECT contract_id,
                token_id,
                metadata_json,
                metadata_json_bytes,
                source_file,
                source_row_number,
                metadata_eligible
         FROM metadata_rows;",
    )?;
    persist_last_duckdb_profile(conn, "materialized durable metadata rows")?;
    conn.execute_batch(
        "DROP VIEW metadata_rows;
         DROP VIEW uri_rows;
         DROP TABLE prepare_uri_metadata_rows;
         ALTER TABLE metadata_rows_materialized RENAME TO metadata_rows;",
    )?;
    Ok(())
}

pub(crate) fn execute_progress_batch(
    conn: &Connection,
    sql: &str,
    progress: &ProgressTracker,
    message: &str,
) -> Result<(), AnalysisError> {
    progress.start_task(message, None, "rows");
    conn.execute_batch(sql)?;
    persist_last_duckdb_profile(conn, message)?;
    progress.finish_task(message);
    progress.step_stage(message);
    Ok(())
}

/// Verify that the Arrow column at `index` is named `name`. Reads bind columns
/// positionally; every SELECT feeding these helpers names its columns to match,
/// so a mismatch means the SQL and reader have desynced. Fail fast with a clear
/// error instead of silently reading the wrong column.
#[cfg(test)]
pub(crate) fn arrow_verify_column_name(
    batch: &duckdb::arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<(), AnalysisError> {
    let schema = batch.schema();
    match schema.fields().get(index) {
        Some(field) if field.name() == name => Ok(()),
        Some(field) => Err(AnalysisError::InvalidData(format!(
            "DuckDB Arrow column at index {index} is {:?}, expected {name}",
            field.name()
        ))),
        None => Err(AnalysisError::InvalidData(format!(
            "DuckDB Arrow column {name} is missing at index {index}"
        ))),
    }
}

#[cfg(test)]
pub(crate) fn arrow_i64_column<'a>(
    batch: &'a duckdb::arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a duckdb::arrow::array::Int64Array, AnalysisError> {
    arrow_verify_column_name(batch, index, name)?;
    batch
        .columns()
        .get(index)
        .and_then(|column| {
            column
                .as_any()
                .downcast_ref::<duckdb::arrow::array::Int64Array>()
        })
        .ok_or_else(|| {
            AnalysisError::InvalidData(format!(
                "DuckDB Arrow column {name} is missing or is not BIGINT"
            ))
        })
}

#[cfg(test)]
pub(crate) fn arrow_string_column<'a>(
    batch: &'a duckdb::arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a duckdb::arrow::array::StringArray, AnalysisError> {
    arrow_verify_column_name(batch, index, name)?;
    batch
        .columns()
        .get(index)
        .and_then(|column| {
            column
                .as_any()
                .downcast_ref::<duckdb::arrow::array::StringArray>()
        })
        .ok_or_else(|| {
            AnalysisError::InvalidData(format!(
                "DuckDB Arrow column {name} is missing or is not VARCHAR"
            ))
        })
}

pub(crate) fn parquet_input_columns(
    conn: &Connection,
    inputs_sql: &str,
) -> Result<std::collections::HashSet<String>, AnalysisError> {
    let mut stmt = conn.prepare(&format!(
        "DESCRIBE SELECT * FROM read_parquet({inputs_sql})"
    ))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut columns = std::collections::HashSet::new();
    for row in rows {
        columns.insert(row?.to_ascii_lowercase());
    }
    Ok(columns)
}

pub(crate) fn metadata_json_projection_expr(columns: &std::collections::HashSet<String>) -> String {
    match (
        columns.contains("metadata_json"),
        columns.contains("metadata_doc"),
    ) {
        (true, true) => {
            "coalesce(nullif(trim(CAST(metadata_json AS VARCHAR)), ''), nullif(trim(CAST(metadata_doc AS VARCHAR)), ''), '')"
                .to_string()
        }
        (true, false) => "coalesce(nullif(trim(CAST(metadata_json AS VARCHAR)), ''), '')".to_string(),
        (false, true) => "coalesce(nullif(trim(CAST(metadata_doc AS VARCHAR)), ''), '')".to_string(),
        (false, false) => "''".to_string(),
    }
}

pub(crate) fn build_uri_key_stats(
    conn: &Connection,
    progress: &ProgressTracker,
    include_cross_chain: bool,
) -> Result<(), AnalysisError> {
    execute_progress_batch(
        conn,
        &build_uri_key_contracts_sql(),
        progress,
        "built URI key contracts",
    )?;

    if include_cross_chain {
        // This is the shared compression boundary for both cross-chain
        // existence and directed pair expansion.  Without it, the full
        // contract/key relation is hash-aggregated twice and every matching
        // NFT row probes all C selected chains.
        execute_progress_batch(
            conn,
            &build_uri_key_chain_stats_sql(),
            progress,
            "built compact URI chain-key stats",
        )?;
    }
    execute_progress_batch(
        conn,
        &build_uri_duplicate_key_stats_sql(include_cross_chain),
        progress,
        "built duplicate-only URI key stats",
    )?;
    if include_cross_chain {
        execute_progress_batch(
            conn,
            &build_uri_cross_chain_keys_sql(),
            progress,
            "built cross-chain URI keys",
        )?;
    }
    conn.execute_batch("DROP TABLE uri_key_contracts;")?;
    Ok(())
}

pub(crate) fn build_uri_key_contracts_sql() -> String {
    "
        CREATE TEMP TABLE uri_key_contracts AS
        SELECT rows.chain_index,
               0::UTINYINT AS key_kind,
               rows.token_uri_norm AS key_value,
               rows.contract_id
        FROM uri_rows rows
        WHERE rows.token_uri_norm <> ''
        GROUP BY rows.chain_index, rows.token_uri_norm, rows.contract_id
        UNION ALL
        SELECT rows.chain_index,
               1::UTINYINT AS key_kind,
               rows.image_uri_norm AS key_value,
               rows.contract_id
        FROM uri_rows rows
        WHERE rows.image_uri_norm <> ''
        GROUP BY rows.chain_index, rows.image_uri_norm, rows.contract_id;
    "
    .to_string()
}

pub(crate) fn build_uri_key_chain_stats_sql() -> String {
    "
        CREATE TEMP TABLE uri_key_chain_stats AS
        SELECT chain_index,
               key_kind,
               key_value,
               count(*) >= 2 AS is_duplicate
        FROM uri_key_contracts
        GROUP BY chain_index, key_kind, key_value;
    "
    .to_string()
}

pub(crate) fn build_uri_duplicate_key_stats_sql(include_cross_chain: bool) -> String {
    let source = if include_cross_chain {
        "uri_key_chain_stats"
    } else {
        "uri_key_contracts"
    };
    format!(
        "
        CREATE TEMP TABLE uri_duplicate_key_stats AS
        SELECT chain_index,
               key_kind,
               key_value,
               {duplicate_value} AS is_duplicate
        FROM {source}
        {duplicate_filter};
        ",
        duplicate_value = if include_cross_chain {
            "is_duplicate"
        } else {
            "true"
        },
        duplicate_filter = if include_cross_chain {
            "WHERE is_duplicate"
        } else {
            "GROUP BY chain_index, key_kind, key_value\n        HAVING count(*) >= 2"
        },
    )
}

pub(crate) fn build_uri_cross_chain_keys_sql() -> String {
    "
        CREATE TEMP TABLE uri_cross_chain_keys AS
        SELECT keys.key_kind,
               keys.key_value
        FROM uri_key_chain_stats keys
        GROUP BY keys.key_kind, keys.key_value
        HAVING count(*) >= 2;
        "
    .to_string()
}

pub(crate) fn build_uri_contract_flags(
    conn: &Connection,
    progress: &ProgressTracker,
    include_cross_chain: bool,
) -> Result<(), AnalysisError> {
    execute_progress_batch(
        conn,
        &build_uri_contract_flags_sql(include_cross_chain),
        progress,
        "built compact URI chain counts",
    )?;
    conn.execute_batch(
        "DROP TABLE uri_duplicate_key_stats;
         DROP TABLE IF EXISTS uri_cross_chain_keys;",
    )?;
    if include_cross_chain {
        execute_progress_batch(
            conn,
            &build_uri_chain_pair_contract_flags_sql(),
            progress,
            "built URI directed chain-pair counts",
        )?;
        conn.execute_batch("DROP TABLE uri_key_chain_stats;")?;
    }
    Ok(())
}

pub(crate) fn build_uri_contract_flags_sql(include_cross_chain: bool) -> String {
    let (cross_key_columns, cross_key_joins) = if include_cross_chain {
        (
            ",
                       ct.key_value IS NOT NULL AS norm_token_cross_chain,
                       ci.key_value IS NOT NULL AS norm_image_cross_chain",
            "
                LEFT JOIN uri_cross_chain_keys ct
                  ON ct.key_kind = 0
                 AND ct.key_value = r.token_uri_norm
                LEFT JOIN uri_cross_chain_keys ci
                  ON ci.key_kind = 1
                 AND ci.key_value = r.image_uri_norm",
        )
    } else {
        ("", "")
    };
    let cross_aggregate_columns = if include_cross_chain {
        format!(
            ",\n                   {}",
            uri_count_sum_columns("norm_cross_chain")
        )
    } else {
        String::new()
    };

    format!(
        "
            CREATE TEMP TABLE uri_contract_flags AS
            WITH rows AS (
                SELECT uri.chain_index,
                       uri.contract_id,
                       uri.token_uri_norm,
                       uri.image_uri_norm
                FROM uri_rows uri
                WHERE (
                      uri.token_uri_norm <> ''
                      OR uri.image_uri_norm <> ''
                  )
            ),
            keyed AS (
                SELECT r.chain_index,
                       r.contract_id,
                       coalesce(nt.is_duplicate, false) AS norm_token_contract,
                       coalesce(ni.is_duplicate, false) AS norm_image_contract
                       {cross_key_columns}
                FROM rows r
                LEFT JOIN uri_duplicate_key_stats nt
                  ON nt.chain_index = r.chain_index
                 AND nt.key_kind = 0
                 AND nt.key_value = r.token_uri_norm
                LEFT JOIN uri_duplicate_key_stats ni
                  ON ni.chain_index = r.chain_index
                 AND ni.key_kind = 1
                 AND ni.key_value = r.image_uri_norm
                {cross_key_joins}
            ),
            contract_flags AS (
                -- Contract-level maxima are required for exact distinct
                -- contract counts, but no downstream consumer needs this
                -- cardinality-sized relation. Feed it directly into the
                -- chain aggregate and materialize only O(C) rows.
                SELECT chain_index,
                       contract_id,
                       {contract_columns}
                FROM keyed
                GROUP BY chain_index, contract_id
            )
            SELECT chain_index,
                   {aggregate_columns}{cross_aggregate_columns}
            FROM contract_flags
            GROUP BY chain_index;
            ",
        contract_columns = uri_contract_metric_columns(include_cross_chain),
        aggregate_columns = uri_count_sum_columns("norm_contract"),
        cross_aggregate_columns = cross_aggregate_columns,
        cross_key_columns = cross_key_columns,
        cross_key_joins = cross_key_joins,
    )
}

pub(crate) fn uri_contract_metric_columns(include_cross_chain: bool) -> String {
    let mut columns = uri_contract_metric_sql(
        "norm_contract",
        "norm_token_contract",
        "norm_image_contract",
    );
    if include_cross_chain {
        columns.push_str(",\n                   ");
        columns.push_str(&uri_contract_metric_sql(
            "norm_cross_chain",
            "norm_token_cross_chain",
            "norm_image_cross_chain",
        ));
    }
    columns
}

pub(crate) fn uri_contract_metric_sql(prefix: &str, token_flag: &str, image_flag: &str) -> String {
    format!(
        "coalesce(sum(CASE WHEN {token_flag} THEN 1 ELSE 0 END), 0)::BIGINT AS {prefix}_v1_nfts,
                   max(CASE WHEN {token_flag} THEN 1 ELSE 0 END)::BIGINT AS {prefix}_v1_contracts,
                   coalesce(sum(CASE WHEN NOT {token_flag} AND {image_flag} THEN 1 ELSE 0 END), 0)::BIGINT AS {prefix}_v2_nfts,
                   max(CASE WHEN NOT {token_flag} AND {image_flag} THEN 1 ELSE 0 END)::BIGINT AS {prefix}_v2_contracts,
                   coalesce(sum(CASE WHEN {token_flag} OR {image_flag} THEN 1 ELSE 0 END), 0)::BIGINT AS {prefix}_v3_nfts,
                   max(CASE WHEN {token_flag} OR {image_flag} THEN 1 ELSE 0 END)::BIGINT AS {prefix}_v3_contracts"
    )
}

pub(crate) fn build_uri_chain_pair_contract_flags_sql() -> String {
    format!(
        "
        CREATE TEMP TABLE uri_chain_pair_contract_flags AS
        WITH rows AS (
            SELECT projected.rowid AS uri_row_id,
                   projected.chain_index AS primary_chain_index,
                   projected.contract_id,
                   token_uri_norm,
                   image_uri_norm
            FROM prepare_uri_metadata_rows projected
            WHERE token_uri_norm <> '' OR image_uri_norm <> ''
        ),
        token_hits AS (
            SELECT r.uri_row_id,
                   r.primary_chain_index,
                   secondary.chain_index AS secondary_chain_index,
                   r.contract_id,
                   true AS norm_token_chain,
                   false AS norm_image_chain
            FROM rows r
            -- `uri_key_chain_stats` is unique by (chain, kind, value), so this
            -- is an output-sensitive hash join: O(rows + emitted hits), not
            -- O(rows * selected chains).
            INNER JOIN uri_key_chain_stats secondary
              ON secondary.key_kind = 0
             AND secondary.key_value = r.token_uri_norm
             AND secondary.chain_index <> r.primary_chain_index
        ),
        image_hits AS (
            SELECT r.uri_row_id,
                   r.primary_chain_index,
                   secondary.chain_index AS secondary_chain_index,
                   r.contract_id,
                   false AS norm_token_chain,
                   true AS norm_image_chain
            FROM rows r
            INNER JOIN uri_key_chain_stats secondary
              ON secondary.key_kind = 1
             AND secondary.key_value = r.image_uri_norm
             AND secondary.chain_index <> r.primary_chain_index
        ),
        keyed AS (
            SELECT uri_row_id,
                   primary_chain_index,
                   secondary_chain_index,
                   contract_id,
                   bool_or(norm_token_chain) AS norm_token_chain,
                   bool_or(norm_image_chain) AS norm_image_chain
            FROM (
                SELECT * FROM token_hits
                UNION ALL
                SELECT * FROM image_hits
            ) hits
            GROUP BY uri_row_id, primary_chain_index, secondary_chain_index, contract_id
        )
        SELECT primary_chain_index,
               secondary_chain_index,
               {aggregate_columns}
        FROM (
            -- As above, keep the contract relation inside the query plan and
            -- persist only O(C^2) directed pair totals.
            SELECT primary_chain_index,
                   secondary_chain_index,
                   contract_id,
                   {metric_columns}
            FROM keyed
            GROUP BY primary_chain_index, secondary_chain_index, contract_id
        ) contract_flags
        GROUP BY primary_chain_index, secondary_chain_index;
        ",
        metric_columns =
            uri_contract_metric_sql("norm_chain", "norm_token_chain", "norm_image_chain"),
        aggregate_columns = uri_count_sum_columns("norm_chain"),
    )
}

pub(crate) fn uri_count_sum_columns(prefix: &str) -> String {
    format!(
        "coalesce(sum({prefix}_v1_nfts), 0)::BIGINT AS {prefix}_v1_nfts,
               coalesce(sum({prefix}_v1_contracts), 0)::BIGINT AS {prefix}_v1_contracts,
               coalesce(sum({prefix}_v2_nfts), 0)::BIGINT AS {prefix}_v2_nfts,
               coalesce(sum({prefix}_v2_contracts), 0)::BIGINT AS {prefix}_v2_contracts,
               coalesce(sum({prefix}_v3_nfts), 0)::BIGINT AS {prefix}_v3_nfts,
               coalesce(sum({prefix}_v3_contracts), 0)::BIGINT AS {prefix}_v3_contracts"
    )
}

pub(crate) fn uri_contract_counts_sql(include_cross_chain: bool) -> String {
    let cross_columns = if include_cross_chain {
        format!(
            ",\n               {}",
            uri_count_columns("norm_cross_chain")
        )
    } else {
        String::new()
    };
    format!(
        "
        SELECT chains.chain,
               {intra_columns}{cross_columns}
        FROM uri_contract_flags flags
        INNER JOIN selected_chains chains USING (chain_index)
        ",
        intra_columns = uri_count_columns("norm_contract"),
    )
}

pub(crate) fn uri_count_columns(prefix: &str) -> String {
    format!(
        "{prefix}_v1_nfts,
               {prefix}_v1_contracts,
               {prefix}_v2_nfts,
               {prefix}_v2_contracts,
               {prefix}_v3_nfts,
               {prefix}_v3_contracts"
    )
}

pub(crate) fn uri_counts_from_row_at(
    row: &duckdb::Row<'_>,
    start: usize,
) -> duckdb::Result<UriCounts> {
    Ok(UriCounts {
        v1_nfts: row.get(start)?,
        v1_contracts: row.get(start + 1)?,
        v2_nfts: row.get(start + 2)?,
        v2_contracts: row.get(start + 3)?,
        v3_nfts: row.get(start + 4)?,
        v3_contracts: row.get(start + 5)?,
    })
}

pub(crate) fn load_uri_contract_counts(
    conn: &Connection,
    include_cross_chain: bool,
) -> Result<HashMap<String, UriContractCounts>, AnalysisError> {
    let mut stmt = conn.prepare(&uri_contract_counts_sql(include_cross_chain))?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            UriContractCounts {
                intra_chain: uri_counts_from_row_at(row, 1)?,
                cross_chain: if include_cross_chain {
                    uri_counts_from_row_at(row, 7)?
                } else {
                    UriCounts::default()
                },
            },
        ))
    })?;
    let mut counts_by_chain = HashMap::new();
    for row in rows {
        let (chain, counts) = row?;
        counts_by_chain.insert(chain, counts);
    }
    Ok(counts_by_chain)
}

pub(crate) fn uri_chain_pair_counts_sql() -> &'static str {
    "
        SELECT primary_chain.chain,
               secondary_chain.chain,
               flags.norm_chain_v1_nfts,
               flags.norm_chain_v1_contracts,
               flags.norm_chain_v2_nfts,
               flags.norm_chain_v2_contracts,
               flags.norm_chain_v3_nfts,
               flags.norm_chain_v3_contracts
        FROM uri_chain_pair_contract_flags flags
        INNER JOIN selected_chains primary_chain
          ON primary_chain.chain_index = flags.primary_chain_index
        INNER JOIN selected_chains secondary_chain
          ON secondary_chain.chain_index = flags.secondary_chain_index
        "
}

pub(crate) fn load_uri_chain_pair_counts(
    conn: &Connection,
) -> Result<HashMap<String, HashMap<String, UriCounts>>, AnalysisError> {
    let mut stmt = conn.prepare(uri_chain_pair_counts_sql())?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            uri_counts_from_row_at(row, 2)?,
        ))
    })?;
    let mut counts_by_pair = HashMap::<String, HashMap<String, UriCounts>>::new();
    for row in rows {
        let (primary_chain, secondary_chain, counts) = row?;
        counts_by_pair
            .entry(primary_chain)
            .or_default()
            .insert(secondary_chain, counts);
    }
    Ok(counts_by_pair)
}

pub(crate) fn load_selected_chains(conn: &Connection) -> Result<Vec<String>, AnalysisError> {
    let mut stmt = conn.prepare("SELECT chain FROM selected_chains ORDER BY chain")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut chains = Vec::new();
    for row in rows {
        chains.push(row?);
    }
    if chains.is_empty() {
        return Err(AnalysisError::InvalidData(
            "Parquet inputs do not contain any chain values".to_string(),
        ));
    }
    Ok(chains)
}

pub(crate) fn load_chain_totals(
    conn: &Connection,
) -> Result<HashMap<String, NameTotals>, AnalysisError> {
    let mut totals = HashMap::new();
    let mut stmt = conn.prepare(chain_totals_sql())?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            NameTotals {
                contracts: row.get(1)?,
                nfts: row.get(2)?,
            },
        ))
    })?;
    for row in rows {
        let (chain, total) = row?;
        totals.insert(chain, total);
    }
    Ok(totals)
}

pub(crate) fn chain_totals_sql() -> &'static str {
    "
        SELECT chain, contract_count, nft_count
        FROM chain_totals
    "
}

pub(crate) fn build_chain_totals_sql() -> &'static str {
    "
        CREATE OR REPLACE TABLE chain_totals AS
        SELECT chain,
               count(*)::BIGINT AS contract_count,
               coalesce(sum(nft_count), 0)::BIGINT AS nft_count
        FROM analysis_contracts
        GROUP BY chain
    "
}
