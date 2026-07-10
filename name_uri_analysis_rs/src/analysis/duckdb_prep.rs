use super::*;

pub(crate) fn configure_duckdb(conn: &Connection, options: &AnalysisOptions) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "
        PRAGMA preserve_insertion_order=false;
        ",
    )?;
    conn.execute(&format!("PRAGMA threads={}", options.threads.max(1)), [])?;
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
        let mut system = System::new();
        system.refresh_memory();
        let available = system.available_memory() as usize;
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
    progress.start_phase("preparing DuckDB tables", 7);
    execute_progress_batch(
        conn,
        &build_analysis_rows_sql(&inputs, &metadata_json_expr),
        progress,
        "materialized DuckDB working projection",
    )?;
    execute_progress_batch(
        conn,
        "
            CREATE TEMP TABLE selected_chains AS
            SELECT DISTINCT lower(trim(CAST(chain AS VARCHAR))) AS chain
            FROM analysis_rows
            WHERE chain <> '';
        ",
        progress,
        "loaded selected chains",
    )?;
    let chains = load_selected_chains(conn)?;
    let include_cross_chain = chains.len() > 1;
    if include_cross_chain {
        progress.add_work(3);
    }
    execute_progress_batch(
        conn,
        &analysis_contracts_sql(),
        progress,
        "materialized contract statistics",
    )?;
    build_uri_key_stats(conn, progress, include_cross_chain, false)?;
    build_uri_contract_flags(conn, progress, include_cross_chain, false)?;
    execute_progress_batch(
        conn,
        "
            CREATE TEMP TABLE name_atoms AS
            WITH atoms AS (
                SELECT chain,
                       name_norm,
                       count(*)::BIGINT AS contract_count,
                       coalesce(sum(nft_count), 0)::BIGINT AS nft_count
                FROM analysis_contracts
                WHERE name_norm IS NOT NULL
                GROUP BY chain, name_norm
            )
            SELECT row_number() OVER ()::BIGINT AS atom_id, *
            FROM atoms
            WHERE name_norm <> '';
        ",
        progress,
        "built name atoms",
    )?;
    progress.finish_phase("DuckDB tables ready");
    Ok(chains)
}

pub(super) fn analysis_contracts_sql() -> String {
    format!(
        "
        CREATE TEMP TABLE analysis_contracts AS
        WITH contract_stats AS (
            SELECT chain,
                   contract_address,
                   count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm,
                   arg_min(
                       rowid,
                       row(token_id, rowid)
                   ) FILTER (WHERE {metadata_eligible}) AS metadata_row_id,
                   arg_min(
                       metadata_json,
                       row(token_id, rowid)
                   ) FILTER (WHERE {metadata_eligible}) AS metadata_json
            FROM analysis_rows
            WHERE contract_address <> ''
            GROUP BY chain, contract_address
        ),
        indexed_contracts AS (
            SELECT *,
                   CASE
                       WHEN metadata_row_id IS NULL THEN NULL
                       ELSE count(*) FILTER (WHERE metadata_row_id IS NOT NULL) OVER (
                           ORDER BY chain, contract_address
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) - 1
                   END AS metadata_contract_index
            FROM contract_stats
        )
        SELECT chain,
               contract_address,
               nft_count,
               name_norm,
               metadata_row_id,
               metadata_json,
               metadata_contract_index
        FROM indexed_contracts;
        ",
        metadata_eligible = metadata_json_eligible_predicate("metadata_json"),
    )
}

pub(crate) fn metadata_json_eligible_predicate(column: &str) -> String {
    // Keep in sync with top_contract_analysis_rs::DuckDbFeatureStore::sql_metadata_json_eligible_predicate
    // and metadata_is_dedup_eligible: trim, non-empty, len<=64KiB, starts with { or [
    let trimmed = format!("trim(coalesce(CAST({column} AS VARCHAR), ''))");
    format!(
        "{trimmed} <> ''
         AND length({trimmed}) <= {MAX_METADATA_BYTES_FOR_DEDUP}
         AND (
             starts_with({trimmed}, '{{')
             OR starts_with({trimmed}, '[')
         )"
    )
}

pub(crate) fn build_analysis_rows_sql(inputs: &str, metadata_json_expr: &str) -> String {
    format!(
        "
            CREATE TEMP TABLE analysis_rows AS
            SELECT lower(trim(CAST(chain AS VARCHAR))) AS chain,
                   CASE
                       WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'
                           THEN trim(CAST(contract_address AS VARCHAR))
                       ELSE lower(trim(CAST(contract_address AS VARCHAR)))
                    END AS contract_address,
                   trim(coalesce(CAST(token_id AS VARCHAR), '')) AS token_id,
                   coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                   coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                   trim(coalesce(CAST(name_norm AS VARCHAR), '')) AS name_norm,
                   trim(coalesce({metadata_json_expr}, '')) AS metadata_json
            FROM read_parquet({inputs})
            WHERE chain IS NOT NULL
              AND trim(CAST(chain AS VARCHAR)) <> '';
            ",
        inputs = inputs,
        metadata_json_expr = metadata_json_expr,
    )
}

pub(crate) fn execute_progress_batch(
    conn: &Connection,
    sql: &str,
    progress: &ProgressTracker,
    message: &str,
) -> Result<(), AnalysisError> {
    progress.set_message(message);
    conn.execute_batch(sql)?;
    progress.step(message);
    Ok(())
}

/// Verify that the Arrow column at `index` is named `name`. Reads bind columns
/// positionally; every SELECT feeding these helpers names its columns to match,
/// so a mismatch means the SQL and reader have desynced. Fail fast with a clear
/// error instead of silently reading the wrong column.
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

pub(crate) fn prepared_table_scope(persist_prepared: bool) -> &'static str {
    if persist_prepared {
        ""
    } else {
        "TEMP "
    }
}

pub(crate) fn build_uri_key_stats(
    conn: &Connection,
    progress: &ProgressTracker,
    include_cross_chain: bool,
    persist_prepared: bool,
) -> Result<(), AnalysisError> {
    execute_progress_batch(
        conn,
        &build_uri_key_contracts_sql(persist_prepared),
        progress,
        "built URI key contracts",
    )?;

    execute_progress_batch(
        conn,
        &build_uri_duplicate_key_stats_sql(persist_prepared),
        progress,
        "built duplicate-only URI key stats",
    )?;
    if include_cross_chain {
        execute_progress_batch(
            conn,
            &build_uri_cross_chain_keys_sql(persist_prepared),
            progress,
            "built cross-chain URI keys",
        )?;
        execute_progress_batch(
            conn,
            &build_uri_key_chain_presence_sql(persist_prepared),
            progress,
            "built URI key chain presence",
        )?;
    }
    Ok(())
}

pub(crate) fn build_uri_key_contracts_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_key_contracts AS
        SELECT r.chain,
               keys.key_kind,
               keys.key_value,
               r.contract_address,
               count(*)::BIGINT AS nft_count
        FROM analysis_rows r
            CROSS JOIN LATERAL (
            VALUES
                ('norm_token', r.token_uri_norm),
                ('norm_image', r.image_uri_norm)
        ) AS keys(key_kind, key_value)
        WHERE r.contract_address <> ''
          AND keys.key_value <> ''
        GROUP BY r.chain, keys.key_kind, keys.key_value, r.contract_address;
    ",
        table_scope = prepared_table_scope(persist_prepared),
    )
}

pub(crate) fn build_uri_duplicate_key_stats_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_duplicate_key_stats AS
        SELECT chain,
               key_kind,
               key_value,
               coalesce(sum(nft_count), 0)::BIGINT AS nft_count,
               count(*)::BIGINT AS contract_count
        FROM uri_key_contracts
        GROUP BY chain, key_kind, key_value
        HAVING coalesce(sum(nft_count), 0) >= 2
            OR count(*) >= 2;
    ",
        table_scope = prepared_table_scope(persist_prepared),
    )
}

pub(crate) fn build_uri_cross_chain_keys_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_cross_chain_keys AS
        SELECT key_kind, key_value
        FROM uri_key_contracts
        GROUP BY key_kind, key_value
        HAVING count(DISTINCT chain) >= 2;
        ",
        table_scope = prepared_table_scope(persist_prepared),
    )
}

pub(crate) fn build_uri_key_chain_presence_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_key_chain_presence AS
        SELECT DISTINCT keys.chain, keys.key_kind, keys.key_value
        FROM uri_key_contracts keys
        INNER JOIN uri_cross_chain_keys cross_keys
          ON cross_keys.key_kind = keys.key_kind
         AND cross_keys.key_value = keys.key_value;
        ",
        table_scope = prepared_table_scope(persist_prepared),
    )
}

pub(crate) fn build_uri_contract_flags(
    conn: &Connection,
    progress: &ProgressTracker,
    include_cross_chain: bool,
    persist_prepared: bool,
) -> Result<(), AnalysisError> {
    execute_progress_batch(
        conn,
        &build_uri_contract_flags_sql(include_cross_chain, persist_prepared),
        progress,
        "built compact URI contract flags",
    )?;
    if include_cross_chain {
        execute_progress_batch(
            conn,
            &build_uri_chain_pair_contract_flags_sql(persist_prepared),
            progress,
            "built URI chain-pair contract flags",
        )?;
    }
    Ok(())
}

pub(crate) fn build_uri_contract_flags_sql(include_cross_chain: bool, persist_prepared: bool) -> String {
    let (cross_key_columns, cross_key_joins) = if include_cross_chain {
        (
            ",
                       coalesce(ct.key_value IS NOT NULL, false) AS norm_token_cross_chain,
                       coalesce(ci.key_value IS NOT NULL, false) AS norm_image_cross_chain",
            "
                LEFT JOIN uri_cross_chain_keys ct
                  ON ct.key_kind = 'norm_token'
                 AND ct.key_value = r.token_uri_norm
                LEFT JOIN uri_cross_chain_keys ci
                  ON ci.key_kind = 'norm_image'
                 AND ci.key_value = r.image_uri_norm",
        )
    } else {
        ("", "")
    };

    format!(
        "
            CREATE {table_scope}TABLE uri_contract_flags AS
            WITH rows AS (
                SELECT chain,
                       contract_address,
                       token_uri_norm,
                       image_uri_norm
                FROM analysis_rows
                WHERE contract_address <> ''
                  AND (
                      token_uri_norm <> ''
                      OR image_uri_norm <> ''
                  )
            ),
            keyed AS (
                SELECT r.chain,
                       r.contract_address,
                       coalesce(nt.contract_count >= 2, false) AS norm_token_contract,
                       coalesce(ni.contract_count >= 2, false) AS norm_image_contract
                       {cross_key_columns}
                FROM rows r
                LEFT JOIN uri_duplicate_key_stats nt
                  ON nt.chain = r.chain
                 AND nt.key_kind = 'norm_token'
                 AND nt.key_value = r.token_uri_norm
                LEFT JOIN uri_duplicate_key_stats ni
                  ON ni.chain = r.chain
                 AND ni.key_kind = 'norm_image'
                 AND ni.key_value = r.image_uri_norm
                {cross_key_joins}
            )
            SELECT chain,
                   contract_address,
                   count(*)::BIGINT AS total_nfts,
                   {contract_columns}
            FROM keyed
            GROUP BY chain, contract_address;
            ",
        contract_columns = uri_contract_metric_columns(include_cross_chain),
        cross_key_columns = cross_key_columns,
        cross_key_joins = cross_key_joins,
        table_scope = prepared_table_scope(persist_prepared),
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

pub(crate) fn build_uri_chain_pair_contract_flags_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_chain_pair_contract_flags AS
        WITH rows AS (
            SELECT rowid AS uri_row_id,
                   chain AS primary_chain,
                   contract_address,
                   token_uri_norm,
                   image_uri_norm
            FROM analysis_rows
            WHERE contract_address <> ''
              AND (token_uri_norm <> '' OR image_uri_norm <> '')
        ),
        token_hits AS (
            SELECT r.uri_row_id,
                   r.primary_chain,
                   presence.chain AS secondary_chain,
                   r.contract_address,
                   true AS norm_token_chain,
                   false AS norm_image_chain
            FROM rows r
            INNER JOIN uri_key_chain_presence presence
              ON presence.key_kind = 'norm_token'
             AND presence.key_value = r.token_uri_norm
             AND presence.chain <> r.primary_chain
        ),
        image_hits AS (
            SELECT r.uri_row_id,
                   r.primary_chain,
                   presence.chain AS secondary_chain,
                   r.contract_address,
                   false AS norm_token_chain,
                   true AS norm_image_chain
            FROM rows r
            INNER JOIN uri_key_chain_presence presence
              ON presence.key_kind = 'norm_image'
             AND presence.key_value = r.image_uri_norm
             AND presence.chain <> r.primary_chain
        ),
        keyed AS (
            SELECT uri_row_id,
                   primary_chain,
                   secondary_chain,
                   contract_address,
                   bool_or(norm_token_chain) AS norm_token_chain,
                   bool_or(norm_image_chain) AS norm_image_chain
            FROM (
                SELECT * FROM token_hits
                UNION ALL
                SELECT * FROM image_hits
            ) hits
            GROUP BY uri_row_id, primary_chain, secondary_chain, contract_address
        )
        SELECT primary_chain,
               secondary_chain,
               contract_address,
               {metric_columns}
        FROM keyed
        GROUP BY primary_chain, secondary_chain, contract_address;
        ",
        table_scope = prepared_table_scope(persist_prepared),
        metric_columns =
            uri_contract_metric_sql("norm_chain", "norm_token_chain", "norm_image_chain"),
    )
}

pub(crate) fn uri_count_sum_columns(prefix: &str) -> String {
    format!(
        "coalesce(sum({prefix}_v1_nfts), 0)::BIGINT,
               coalesce(sum({prefix}_v1_contracts), 0)::BIGINT,
               coalesce(sum({prefix}_v2_nfts), 0)::BIGINT,
               coalesce(sum({prefix}_v2_contracts), 0)::BIGINT,
               coalesce(sum({prefix}_v3_nfts), 0)::BIGINT,
               coalesce(sum({prefix}_v3_contracts), 0)::BIGINT"
    )
}

pub(crate) fn uri_contract_counts_sql(include_cross_chain: bool) -> String {
    let cross_columns = if include_cross_chain {
        format!(",\n               {}", uri_count_sum_columns("norm_cross_chain"))
    } else {
        String::new()
    };
    format!(
        "
        SELECT chain,
               {intra_columns}{cross_columns}
        FROM uri_contract_flags
        GROUP BY chain
        ",
        intra_columns = uri_count_sum_columns("norm_contract"),
    )
}

pub(crate) fn uri_counts_from_row_at(row: &duckdb::Row<'_>, start: usize) -> duckdb::Result<UriCounts> {
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
        SELECT primary_chain,
               secondary_chain,
               coalesce(sum(norm_chain_v1_nfts), 0)::BIGINT,
               coalesce(sum(norm_chain_v1_contracts), 0)::BIGINT,
               coalesce(sum(norm_chain_v2_nfts), 0)::BIGINT,
               coalesce(sum(norm_chain_v2_contracts), 0)::BIGINT,
               coalesce(sum(norm_chain_v3_nfts), 0)::BIGINT,
               coalesce(sum(norm_chain_v3_contracts), 0)::BIGINT
        FROM uri_chain_pair_contract_flags
        GROUP BY primary_chain, secondary_chain
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

pub(crate) fn load_chain_totals(conn: &Connection) -> Result<HashMap<String, NameTotals>, AnalysisError> {
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
        SELECT chain,
               count(*)::BIGINT AS contract_count,
               coalesce(sum(nft_count), 0)::BIGINT AS nft_count
        FROM analysis_contracts
        GROUP BY chain
    "
}
