fn configure_duckdb(conn: &Connection, options: &AnalysisOptions) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "
        PRAGMA preserve_insertion_order=false;
        ",
    )?;
    conn.execute(&format!("PRAGMA threads={}", options.threads.max(1)), [])?;
    let total_budget = total_memory_budget_bytes(&options.memory_limit)?;
    let mut memory_guard = MemoryGuard::new(total_budget);
    set_duckdb_memory_limit_for_process_budget(conn, &mut memory_guard, total_budget)?;
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
    let total_budget = total_memory_budget_bytes(&options.memory_limit)?;
    let mut memory_guard = MemoryGuard::new(total_budget);
    progress.start_phase("preparing DuckDB tables", 10);
    execute_duckdb_progress_batch(
        conn,
        "
        DROP TABLE IF EXISTS analysis_rows;
        DROP TABLE IF EXISTS selected_chains;
        DROP TABLE IF EXISTS uri_key_contracts;
        DROP TABLE IF EXISTS uri_key_stats;
        DROP TABLE IF EXISTS uri_key_chain_counts;
        DROP TABLE IF EXISTS uri_contract_flags;
        DROP TABLE IF EXISTS contract_names;
        DROP TABLE IF EXISTS name_atoms;
        ",
        progress,
        "dropped stale DuckDB tables",
        &mut memory_guard,
        total_budget,
    )?;
    execute_duckdb_progress_batch(
        conn,
        &format!(
            "
            CREATE TEMP TABLE analysis_rows AS
            SELECT lower(trim(CAST(chain AS VARCHAR))) AS chain,
                   lower(trim(CAST(contract_address AS VARCHAR))) AS contract_address,
                   coalesce(CAST(token_id AS VARCHAR), '') AS token_id,
                   trim(coalesce(CAST(token_uri AS VARCHAR), '')) AS token_uri,
                   trim(coalesce(CAST(image_uri AS VARCHAR), '')) AS image_uri,
                   coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                   coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                   coalesce(CAST(name AS VARCHAR), '') AS name,
                   trim(coalesce(CAST(name_norm AS VARCHAR), '')) AS name_norm
            FROM read_parquet({inputs})
            WHERE chain IS NOT NULL
              AND trim(CAST(chain AS VARCHAR)) <> '';
            ",
            inputs = inputs,
        ),
        progress,
        "materialized DuckDB working projection",
        &mut memory_guard,
        total_budget,
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
        &mut memory_guard,
        total_budget,
    )?;
    build_uri_key_stats(conn, progress, &mut memory_guard, total_budget)?;
    build_uri_contract_flags(conn, progress, &mut memory_guard, total_budget)?;
    execute_duckdb_progress_batch(
        conn,
        "
            CREATE TEMP TABLE contract_names AS
            WITH ranked AS (
                SELECT chain,
                       contract_address,
                       name,
                       name_norm,
                       count(*) OVER (
                           PARTITION BY chain, contract_address
                       )::BIGINT AS nft_count,
                       row_number() OVER (
                           PARTITION BY chain, contract_address
                           ORDER BY CASE WHEN name_norm <> '' THEN 0 ELSE 1 END,
                                    token_id DESC
                       ) AS rn
                FROM analysis_rows
                WHERE contract_address <> ''
            )
            SELECT chain, contract_address, nft_count, name, name_norm
            FROM ranked
            WHERE rn = 1
              AND name_norm <> '';
        ",
        progress,
        "materialized contract names",
        &mut memory_guard,
        total_budget,
    )?;
    execute_duckdb_progress_batch(
        conn,
        "
            CREATE TEMP TABLE name_atoms AS
            WITH atoms AS (
                SELECT chain,
                       name_norm,
                       min(name) AS sample_name,
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
        &mut memory_guard,
        total_budget,
    )?;
    let chains = load_selected_chains(conn)?;
    progress.finish_phase("DuckDB tables ready");
    Ok(chains)
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
    memory_guard: &mut MemoryGuard,
    desired_duckdb_bytes: usize,
) -> Result<(), AnalysisError> {
    set_duckdb_memory_limit_for_process_budget(conn, memory_guard, desired_duckdb_bytes)?;
    execute_progress_batch(conn, sql, progress, message)
}

fn build_uri_key_stats(
    conn: &Connection,
    progress: &ProgressTracker,
    memory_guard: &mut MemoryGuard,
    desired_duckdb_bytes: usize,
) -> Result<(), AnalysisError> {
    execute_duckdb_progress_batch(
        conn,
        "
            CREATE TEMP TABLE uri_key_contracts (
                chain VARCHAR,
                key_kind VARCHAR,
                key_value VARCHAR,
                contract_address VARCHAR,
                nft_count BIGINT
            );
        ",
        progress,
        "created URI key contract table",
        memory_guard,
        desired_duckdb_bytes,
    )?;

    for (key_kind, column_name) in [
        ("strict_token", "token_uri"),
        ("strict_image", "image_uri"),
        ("norm_token", "token_uri_norm"),
        ("norm_image", "image_uri_norm"),
    ] {
        insert_uri_key_contracts(
            conn,
            progress,
            memory_guard,
            desired_duckdb_bytes,
            key_kind,
            column_name,
        )?;
    }

    execute_duckdb_progress_batch(
        conn,
        "
            CREATE TEMP TABLE uri_key_stats AS
            SELECT chain,
                   key_kind,
                   key_value,
                   coalesce(sum(nft_count), 0)::BIGINT AS nft_count,
                   count(*)::BIGINT AS contract_count
            FROM uri_key_contracts
            GROUP BY chain, key_kind, key_value;
        ",
        progress,
        "built URI key stats",
        memory_guard,
        desired_duckdb_bytes,
    )?;
    execute_duckdb_progress_batch(
        conn,
        "
            CREATE TEMP TABLE uri_key_chain_counts AS
            SELECT key_kind,
                   key_value,
                   count(*)::BIGINT AS chain_count
            FROM uri_key_stats
            GROUP BY key_kind, key_value;
        ",
        progress,
        "built URI cross-chain key stats",
        memory_guard,
        desired_duckdb_bytes,
    )?;
    Ok(())
}

fn insert_uri_key_contracts(
    conn: &Connection,
    progress: &ProgressTracker,
    memory_guard: &mut MemoryGuard,
    desired_duckdb_bytes: usize,
    key_kind: &str,
    column_name: &str,
) -> Result<(), AnalysisError> {
    execute_duckdb_progress_batch(
        conn,
        &format!(
            "
            INSERT INTO uri_key_contracts
            SELECT chain,
                   '{key_kind}' AS key_kind,
                   {column_name} AS key_value,
                   contract_address,
                   count(*)::BIGINT AS nft_count
            FROM analysis_rows
            WHERE contract_address <> ''
              AND {column_name} <> ''
            GROUP BY chain, {column_name}, contract_address;
            "
        ),
        progress,
        &format!("built URI key contracts {key_kind}"),
        memory_guard,
        desired_duckdb_bytes,
    )
}

fn build_uri_contract_flags(
    conn: &Connection,
    progress: &ProgressTracker,
    memory_guard: &mut MemoryGuard,
    desired_duckdb_bytes: usize,
) -> Result<(), AnalysisError> {
    execute_duckdb_progress_batch(
        conn,
        &format!(
            "
            CREATE TEMP TABLE uri_contract_flags AS
            WITH rows AS (
                SELECT chain,
                       contract_address,
                       token_uri,
                       image_uri,
                       token_uri_norm,
                       image_uri_norm
                FROM analysis_rows
                WHERE contract_address <> ''
                  AND (
                      token_uri <> ''
                      OR image_uri <> ''
                      OR token_uri_norm <> ''
                      OR image_uri_norm <> ''
                  )
            ),
            keyed AS (
                SELECT r.chain,
                       r.contract_address,
                       coalesce(st.nft_count >= 2, false) AS strict_token_any,
                       coalesce(si.nft_count >= 2, false) AS strict_image_any,
                       coalesce(st.contract_count >= 2, false) AS strict_token_contract,
                       coalesce(si.contract_count >= 2, false) AS strict_image_contract,
                       coalesce(stc.chain_count >= 2, false) AS strict_token_chain,
                       coalesce(sic.chain_count >= 2, false) AS strict_image_chain,
                       coalesce(nt.nft_count >= 2, false) AS norm_token_any,
                       coalesce(ni.nft_count >= 2, false) AS norm_image_any,
                       coalesce(nt.contract_count >= 2, false) AS norm_token_contract,
                       coalesce(ni.contract_count >= 2, false) AS norm_image_contract,
                       coalesce(ntc.chain_count >= 2, false) AS norm_token_chain,
                       coalesce(nic.chain_count >= 2, false) AS norm_image_chain
                FROM rows r
                LEFT JOIN uri_key_stats st
                  ON st.chain = r.chain
                 AND st.key_kind = 'strict_token'
                 AND st.key_value = r.token_uri
                LEFT JOIN uri_key_stats si
                  ON si.chain = r.chain
                 AND si.key_kind = 'strict_image'
                 AND si.key_value = r.image_uri
                LEFT JOIN uri_key_chain_counts stc
                  ON stc.key_kind = 'strict_token'
                 AND stc.key_value = r.token_uri
                LEFT JOIN uri_key_chain_counts sic
                  ON sic.key_kind = 'strict_image'
                 AND sic.key_value = r.image_uri
                LEFT JOIN uri_key_stats nt
                  ON nt.chain = r.chain
                 AND nt.key_kind = 'norm_token'
                 AND nt.key_value = r.token_uri_norm
                LEFT JOIN uri_key_stats ni
                  ON ni.chain = r.chain
                 AND ni.key_kind = 'norm_image'
                 AND ni.key_value = r.image_uri_norm
                LEFT JOIN uri_key_chain_counts ntc
                  ON ntc.key_kind = 'norm_token'
                 AND ntc.key_value = r.token_uri_norm
                LEFT JOIN uri_key_chain_counts nic
                  ON nic.key_kind = 'norm_image'
                 AND nic.key_value = r.image_uri_norm
            )
            SELECT chain,
                   contract_address,
                   count(*)::BIGINT AS total_nfts,
                   {contract_columns}
            FROM keyed
            GROUP BY chain, contract_address;
            ",
            contract_columns = uri_contract_metric_columns(),
        ),
        progress,
        "built compact URI contract flags",
        memory_guard,
        desired_duckdb_bytes,
    )
}

fn uri_contract_metric_columns() -> String {
    [
        uri_contract_metric_sql("strict_any", "strict_token_any", "strict_image_any"),
        uri_contract_metric_sql(
            "strict_contract",
            "strict_token_contract",
            "strict_image_contract",
        ),
        uri_contract_metric_sql("strict_chain", "strict_token_chain", "strict_image_chain"),
        uri_contract_metric_sql("norm_any", "norm_token_any", "norm_image_any"),
        uri_contract_metric_sql(
            "norm_contract",
            "norm_token_contract",
            "norm_image_contract",
        ),
        uri_contract_metric_sql("norm_chain", "norm_token_chain", "norm_image_chain"),
    ]
    .join(",\n                   ")
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

