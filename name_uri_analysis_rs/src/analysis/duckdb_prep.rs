fn configure_duckdb(conn: &Connection, options: &AnalysisOptions) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "
        PRAGMA preserve_insertion_order=false;
        ",
    )?;
    conn.execute(&format!("PRAGMA threads={}", options.threads.max(1)), [])?;
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

fn prepare_base_tables(
    conn: &Connection,
    options: &AnalysisOptions,
    progress: &ProgressTracker,
) -> Result<Vec<String>, AnalysisError> {
    let inputs = parquet_input_sql(&options.parquet_inputs);
    let input_columns = parquet_input_columns(conn, &inputs)?;
    let metadata_json_expr = metadata_json_projection_expr(&input_columns);
    progress.start_phase("preparing DuckDB tables", 7);
    execute_duckdb_progress_batch(
        conn,
        &build_analysis_rows_sql(&inputs, &metadata_json_expr),
        progress,
        "materialized DuckDB working projection",
    )?;
    execute_duckdb_progress_batch(
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
    build_uri_key_stats(conn, progress, include_cross_chain, false)?;
    build_uri_contract_flags(conn, progress, include_cross_chain, false)?;
    execute_duckdb_progress_batch(
        conn,
        "
            CREATE TEMP TABLE contract_names AS
            SELECT chain,
                   contract_address,
                   count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm
            FROM analysis_rows
            WHERE contract_address <> ''
            GROUP BY chain, contract_address
            HAVING min(nullif(name_norm, '')) IS NOT NULL;
        ",
        progress,
        "materialized contract names",
    )?;
    execute_duckdb_progress_batch(
        conn,
        "
            CREATE TEMP TABLE name_atoms AS
            WITH atoms AS (
                SELECT chain,
                       name_norm,
                       count(*)::BIGINT AS contract_count,
                       coalesce(sum(nft_count), 0)::BIGINT AS nft_count
                FROM contract_names
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

fn build_analysis_rows_sql(inputs: &str, metadata_json_expr: &str) -> String {
    format!(
        "
            CREATE TEMP TABLE analysis_rows AS
            SELECT lower(trim(CAST(chain AS VARCHAR))) AS chain,
                   lower(trim(CAST(contract_address AS VARCHAR))) AS contract_address,
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

fn execute_progress_batch(
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

fn execute_duckdb_progress_batch(
    conn: &Connection,
    sql: &str,
    progress: &ProgressTracker,
    message: &str,
) -> Result<(), AnalysisError> {
    execute_progress_batch(conn, sql, progress, message)
}

fn parquet_input_columns(
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

fn metadata_json_projection_expr(columns: &std::collections::HashSet<String>) -> String {
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

fn prepared_table_scope(persist_prepared: bool) -> &'static str {
    if persist_prepared {
        ""
    } else {
        "TEMP "
    }
}

fn build_uri_key_stats(
    conn: &Connection,
    progress: &ProgressTracker,
    _include_cross_chain: bool,
    persist_prepared: bool,
) -> Result<(), AnalysisError> {
    execute_duckdb_progress_batch(
        conn,
        &build_uri_key_contracts_sql(persist_prepared),
        progress,
        "built URI key contracts",
    )?;

    execute_duckdb_progress_batch(
        conn,
        &build_uri_duplicate_key_stats_sql(persist_prepared),
        progress,
        "built duplicate-only URI key stats",
    )?;
    Ok(())
}

fn build_uri_key_contracts_sql(persist_prepared: bool) -> String {
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

fn build_uri_duplicate_key_stats_sql(persist_prepared: bool) -> String {
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

fn build_uri_contract_flags(
    conn: &Connection,
    progress: &ProgressTracker,
    _include_cross_chain: bool,
    persist_prepared: bool,
) -> Result<(), AnalysisError> {
    execute_duckdb_progress_batch(
        conn,
        &build_uri_contract_flags_sql(false, persist_prepared),
        progress,
        "built compact URI contract flags",
    )
}

fn build_uri_contract_flags_sql(include_cross_chain: bool, persist_prepared: bool) -> String {
    let _ = include_cross_chain;

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
                FROM rows r
                LEFT JOIN uri_duplicate_key_stats nt
                  ON nt.chain = r.chain
                 AND nt.key_kind = 'norm_token'
                 AND nt.key_value = r.token_uri_norm
                LEFT JOIN uri_duplicate_key_stats ni
                  ON ni.chain = r.chain
                 AND ni.key_kind = 'norm_image'
                 AND ni.key_value = r.image_uri_norm
            )
            SELECT chain,
                   contract_address,
                   count(*)::BIGINT AS total_nfts,
                   {contract_columns}
            FROM keyed
            GROUP BY chain, contract_address;
            ",
        contract_columns = uri_contract_metric_columns(include_cross_chain),
        table_scope = prepared_table_scope(persist_prepared),
    )
}

fn uri_contract_metric_columns(include_cross_chain: bool) -> String {
    let _ = include_cross_chain;
    uri_contract_metric_sql(
        "norm_contract",
        "norm_token_contract",
        "norm_image_contract",
    )
}

fn uri_contract_metric_sql(prefix: &str, token_flag: &str, image_flag: &str) -> String {
    format!(
        "coalesce(sum(CASE WHEN {token_flag} THEN 1 ELSE 0 END), 0)::BIGINT AS {prefix}_v1_nfts,
                   max(CASE WHEN {token_flag} THEN 1 ELSE 0 END)::BIGINT AS {prefix}_v1_contracts,
                   coalesce(sum(CASE WHEN NOT {token_flag} AND {image_flag} THEN 1 ELSE 0 END), 0)::BIGINT AS {prefix}_v2_nfts,
                   max(CASE WHEN NOT {token_flag} AND {image_flag} THEN 1 ELSE 0 END)::BIGINT AS {prefix}_v2_contracts,
                   coalesce(sum(CASE WHEN {token_flag} OR {image_flag} THEN 1 ELSE 0 END), 0)::BIGINT AS {prefix}_v3_nfts,
                   max(CASE WHEN {token_flag} OR {image_flag} THEN 1 ELSE 0 END)::BIGINT AS {prefix}_v3_contracts"
    )
}

fn uri_counts_from_contract_flags(
    conn: &Connection,
    chain: &str,
    prefix: &str,
) -> Result<UriCounts, AnalysisError> {
    let sql = format!(
        "
        SELECT coalesce(sum(total_nfts), 0)::BIGINT,
               count(*)::BIGINT,
               coalesce(sum({prefix}_v1_nfts), 0)::BIGINT,
               coalesce(sum({prefix}_v1_contracts), 0)::BIGINT,
               coalesce(sum({prefix}_v2_nfts), 0)::BIGINT,
               coalesce(sum({prefix}_v2_contracts), 0)::BIGINT,
               coalesce(sum({prefix}_v3_nfts), 0)::BIGINT,
               coalesce(sum({prefix}_v3_contracts), 0)::BIGINT
        FROM uri_contract_flags
        WHERE chain = ?
        "
    );
    conn.query_row(&sql, params![chain], uri_counts_from_row)
        .map_err(AnalysisError::from)
}

fn load_selected_chains(conn: &Connection) -> Result<Vec<String>, AnalysisError> {
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
