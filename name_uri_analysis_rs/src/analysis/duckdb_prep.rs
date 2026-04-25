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
    let persist_prepared = options.persist_prepared || options.reuse_prepared;
    let parquet_fingerprint = parquet_inputs_fingerprint(&options.parquet_inputs)?;
    let total_budget = total_memory_budget_bytes(&options.memory_limit)?;
    let mut memory_guard = MemoryGuard::new(total_budget);
    progress.start_phase("preparing DuckDB tables", 10);
    if options.reuse_prepared
        && prepared_tables_can_be_reused(conn, &parquet_fingerprint, persist_prepared)?
    {
        let chains = load_selected_chains(conn)?;
        progress.step("reused persisted DuckDB tables");
        progress.finish_phase("DuckDB tables ready");
        return Ok(chains);
    }
    execute_duckdb_progress_batch(
        conn,
        &drop_prepared_tables_sql(persist_prepared),
        progress,
        "dropped stale DuckDB tables",
        &mut memory_guard,
        total_budget,
    )?;
    execute_duckdb_progress_batch(
        conn,
        &format!(
            "
            CREATE {table_scope}TABLE analysis_rows AS
            SELECT lower(trim(CAST(chain AS VARCHAR))) AS chain,
                   lower(trim(CAST(contract_address AS VARCHAR))) AS contract_address,
                   trim(coalesce(CAST(token_uri AS VARCHAR), '')) AS token_uri,
                   trim(coalesce(CAST(image_uri AS VARCHAR), '')) AS image_uri,
                   coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                   coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                   trim(coalesce(CAST(name_norm AS VARCHAR), '')) AS name_norm
            FROM read_parquet({inputs})
            WHERE chain IS NOT NULL
              AND trim(CAST(chain AS VARCHAR)) <> '';
            ",
            inputs = inputs,
            table_scope = prepared_table_scope(persist_prepared),
        ),
        progress,
        "materialized DuckDB working projection",
        &mut memory_guard,
        total_budget,
    )?;
    execute_duckdb_progress_batch(
        conn,
        &format!(
            "
            CREATE {table_scope}TABLE selected_chains AS
            SELECT DISTINCT lower(trim(CAST(chain AS VARCHAR))) AS chain
            FROM analysis_rows
            WHERE chain <> '';
        ",
            table_scope = prepared_table_scope(persist_prepared),
        ),
        progress,
        "loaded selected chains",
        &mut memory_guard,
        total_budget,
    )?;
    let chains = load_selected_chains(conn)?;
    let include_cross_chain = chains.len() > 1;
    build_uri_key_stats(
        conn,
        progress,
        &mut memory_guard,
        total_budget,
        include_cross_chain,
        persist_prepared,
    )?;
    build_uri_contract_flags(
        conn,
        progress,
        &mut memory_guard,
        total_budget,
        include_cross_chain,
        persist_prepared,
    )?;
    execute_duckdb_progress_batch(
        conn,
        &format!(
            "
            CREATE {table_scope}TABLE contract_names AS
            SELECT chain,
                   contract_address,
                   count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm
            FROM analysis_rows
            WHERE contract_address <> ''
            GROUP BY chain, contract_address
            HAVING min(nullif(name_norm, '')) IS NOT NULL;
        ",
            table_scope = prepared_table_scope(persist_prepared),
        ),
        progress,
        "materialized contract names",
        &mut memory_guard,
        total_budget,
    )?;
    execute_duckdb_progress_batch(
        conn,
        &format!(
            "
            CREATE {table_scope}TABLE name_atoms AS
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
            table_scope = prepared_table_scope(persist_prepared),
        ),
        progress,
        "built name atoms",
        &mut memory_guard,
        total_budget,
    )?;
    if persist_prepared {
        write_prepared_metadata(conn, &parquet_fingerprint)?;
        progress.step("wrote prepared table metadata");
    }
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

const PREPARED_SCHEMA_VERSION: &str = "name-uri-analysis-prepare-v2";
const PREPARED_METADATA_TABLE: &str = "analysis_prepared_metadata";

fn prepared_table_scope(persist_prepared: bool) -> &'static str {
    if persist_prepared {
        ""
    } else {
        "TEMP "
    }
}

fn drop_prepared_tables_sql(persist_prepared: bool) -> String {
    if !persist_prepared {
        return String::new();
    }
    let mut sql = "
        DROP TABLE IF EXISTS analysis_rows;
        DROP TABLE IF EXISTS selected_chains;
        DROP TABLE IF EXISTS uri_key_contracts;
        DROP TABLE IF EXISTS uri_key_stats;
        DROP TABLE IF EXISTS uri_duplicate_key_stats;
        DROP TABLE IF EXISTS uri_key_chain_counts;
        DROP TABLE IF EXISTS uri_duplicate_key_chain_counts;
        DROP TABLE IF EXISTS uri_contract_flags;
        DROP TABLE IF EXISTS contract_names;
        DROP TABLE IF EXISTS name_atoms;
        "
    .to_string();
    sql.push_str("DROP TABLE IF EXISTS analysis_prepared_metadata;");
    sql
}

fn prepared_tables_can_be_reused(
    conn: &Connection,
    parquet_fingerprint: &str,
    persist_prepared: bool,
) -> Result<bool, AnalysisError> {
    if !persist_prepared {
        return Ok(false);
    }
    if prepared_metadata_value(conn, "schema_version")?.as_deref() != Some(PREPARED_SCHEMA_VERSION)
    {
        return Ok(false);
    }
    if prepared_metadata_value(conn, "parquet_fingerprint")?.as_deref() != Some(parquet_fingerprint)
    {
        return Ok(false);
    }
    for table in [
        "analysis_rows",
        "selected_chains",
        "uri_key_contracts",
        "uri_duplicate_key_stats",
        "uri_contract_flags",
        "contract_names",
        "name_atoms",
    ] {
        if !table_exists(conn, table)? {
            return Ok(false);
        }
    }
    let chain_count = selected_chain_count(conn)?;
    if chain_count == 0 {
        return Ok(false);
    }
    if chain_count > 1 && !table_exists(conn, "uri_duplicate_key_chain_counts")? {
        return Ok(false);
    }
    Ok(true)
}

fn prepared_metadata_value(conn: &Connection, key: &str) -> Result<Option<String>, AnalysisError> {
    if !table_exists(conn, PREPARED_METADATA_TABLE)? {
        return Ok(None);
    }
    let mut stmt = conn.prepare(
        "SELECT value FROM analysis_prepared_metadata WHERE key = ? LIMIT 1",
    )?;
    let mut rows = stmt.query(params![key])?;
    if let Some(row) = rows.next()? {
        Ok(Some(row.get(0)?))
    } else {
        Ok(None)
    }
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, AnalysisError> {
    let count: i64 = conn.query_row(
        "SELECT count(*)::BIGINT FROM information_schema.tables WHERE table_name = ?",
        params![table_name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn selected_chain_count(conn: &Connection) -> Result<i64, AnalysisError> {
    let count: i64 = conn.query_row(
        "SELECT count(*)::BIGINT FROM selected_chains",
        [],
        |row| row.get(0),
    )?;
    Ok(count)
}

fn write_prepared_metadata(
    conn: &Connection,
    parquet_fingerprint: &str,
) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS analysis_prepared_metadata;
        CREATE TABLE analysis_prepared_metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR
        );
        ",
    )?;
    conn.execute(
        "INSERT INTO analysis_prepared_metadata VALUES (?, ?), (?, ?)",
        params![
            "schema_version",
            PREPARED_SCHEMA_VERSION,
            "parquet_fingerprint",
            parquet_fingerprint
        ],
    )?;
    Ok(())
}

fn parquet_inputs_fingerprint(paths: &[PathBuf]) -> Result<String, AnalysisError> {
    let mut parts = Vec::with_capacity(paths.len());
    for path in paths {
        let metadata = fs::metadata(path)?;
        let modified = metadata
            .modified()?
            .duration_since(UNIX_EPOCH)
            .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
        let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.clone());
        parts.push(format!(
            "{}|{}|{}|{}",
            canonical.display().to_string().replace('\\', "/"),
            metadata.len(),
            modified.as_secs(),
            modified.subsec_nanos()
        ));
    }
    Ok(parts.join("\n"))
}

fn build_uri_key_stats(
    conn: &Connection,
    progress: &ProgressTracker,
    memory_guard: &mut MemoryGuard,
    desired_duckdb_bytes: usize,
    include_cross_chain: bool,
    persist_prepared: bool,
) -> Result<(), AnalysisError> {
    execute_duckdb_progress_batch(
        conn,
        &build_uri_key_contracts_sql(persist_prepared),
        progress,
        "built URI key contracts",
        memory_guard,
        desired_duckdb_bytes,
    )?;

    execute_duckdb_progress_batch(
        conn,
        &build_uri_duplicate_key_stats_sql(persist_prepared),
        progress,
        "built duplicate-only URI key stats",
        memory_guard,
        desired_duckdb_bytes,
    )?;
    if include_cross_chain {
        execute_duckdb_progress_batch(
            conn,
            &build_uri_duplicate_key_chain_counts_sql(persist_prepared),
            progress,
            "built duplicate-only URI cross-chain key stats",
            memory_guard,
            desired_duckdb_bytes,
        )?;
    }
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
                ('strict_token', r.token_uri),
                ('strict_image', r.image_uri),
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

fn build_uri_duplicate_key_chain_counts_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_duplicate_key_chain_counts AS
        SELECT key_kind,
               key_value,
               count(DISTINCT chain)::BIGINT AS chain_count
        FROM uri_key_contracts
        GROUP BY key_kind, key_value
        HAVING count(DISTINCT chain) >= 2;
    ",
        table_scope = prepared_table_scope(persist_prepared),
    )
}

fn build_uri_contract_flags(
    conn: &Connection,
    progress: &ProgressTracker,
    memory_guard: &mut MemoryGuard,
    desired_duckdb_bytes: usize,
    include_cross_chain: bool,
    persist_prepared: bool,
) -> Result<(), AnalysisError> {
    execute_duckdb_progress_batch(
        conn,
        &build_uri_contract_flags_sql(include_cross_chain, persist_prepared),
        progress,
        "built compact URI contract flags",
        memory_guard,
        desired_duckdb_bytes,
    )
}

fn build_uri_contract_flags_sql(include_cross_chain: bool, persist_prepared: bool) -> String {
    let cross_select_columns = if include_cross_chain {
        ",
                       coalesce(stc.chain_count >= 2, false) AS strict_token_chain,
                       coalesce(sic.chain_count >= 2, false) AS strict_image_chain,
                       coalesce(ntc.chain_count >= 2, false) AS norm_token_chain,
                       coalesce(nic.chain_count >= 2, false) AS norm_image_chain"
    } else {
        ""
    };
    let cross_joins = if include_cross_chain {
        "
                LEFT JOIN uri_duplicate_key_chain_counts stc
                  ON stc.key_kind = 'strict_token'
                 AND stc.key_value = r.token_uri
                LEFT JOIN uri_duplicate_key_chain_counts sic
                  ON sic.key_kind = 'strict_image'
                 AND sic.key_value = r.image_uri
                LEFT JOIN uri_duplicate_key_chain_counts ntc
                  ON ntc.key_kind = 'norm_token'
                 AND ntc.key_value = r.token_uri_norm
                LEFT JOIN uri_duplicate_key_chain_counts nic
                  ON nic.key_kind = 'norm_image'
                 AND nic.key_value = r.image_uri_norm"
    } else {
        ""
    };

    format!(
        "
            CREATE {table_scope}TABLE uri_contract_flags AS
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
                       coalesce(nt.nft_count >= 2, false) AS norm_token_any,
                       coalesce(ni.nft_count >= 2, false) AS norm_image_any,
                       coalesce(nt.contract_count >= 2, false) AS norm_token_contract,
                       coalesce(ni.contract_count >= 2, false) AS norm_image_contract{cross_select_columns}
                FROM rows r
                LEFT JOIN uri_duplicate_key_stats st
                  ON st.chain = r.chain
                 AND st.key_kind = 'strict_token'
                 AND st.key_value = r.token_uri
                LEFT JOIN uri_duplicate_key_stats si
                  ON si.chain = r.chain
                 AND si.key_kind = 'strict_image'
                 AND si.key_value = r.image_uri
                LEFT JOIN uri_duplicate_key_stats nt
                  ON nt.chain = r.chain
                 AND nt.key_kind = 'norm_token'
                 AND nt.key_value = r.token_uri_norm
                LEFT JOIN uri_duplicate_key_stats ni
                  ON ni.chain = r.chain
                 AND ni.key_kind = 'norm_image'
                 AND ni.key_value = r.image_uri_norm{cross_joins}
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
    let mut columns = vec![
        uri_contract_metric_sql("strict_any", "strict_token_any", "strict_image_any"),
        uri_contract_metric_sql(
            "strict_contract",
            "strict_token_contract",
            "strict_image_contract",
        ),
        uri_contract_metric_sql("norm_any", "norm_token_any", "norm_image_any"),
        uri_contract_metric_sql(
            "norm_contract",
            "norm_token_contract",
            "norm_image_contract",
        ),
    ];
    if include_cross_chain {
        columns.extend([
            uri_contract_metric_sql("strict_chain", "strict_token_chain", "strict_image_chain"),
            uri_contract_metric_sql("norm_chain", "norm_token_chain", "norm_image_chain"),
        ]);
    }
    columns.join(",\n                   ")
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
