use super::*;

#[test]
fn analysis_connection_uses_persistent_work_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stage.duckdb");

    let connection = open_analysis_connection(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE persisted(value INTEGER);")
        .unwrap();
    drop(connection);

    assert!(path.exists());
}

#[test]
fn metadata_projection_keeps_token_id_for_verification() {
    let sql = build_uri_metadata_stream_sql(
        "'sample.parquet'",
        "metadata_json",
        "CAST(filename AS VARCHAR)",
    );

    assert!(sql.contains(" AS token_id,"));
    assert!(sql.contains("metadata_json"));
    assert!(sql.contains("filename = true"));
    assert!(sql.contains("file_row_number = true"));
    assert!(sql.contains("source_file"));
    assert!(sql.contains("source_row_number"));
}

#[test]
fn metadata_source_ids_follow_cli_file_order() {
    let paths = [PathBuf::from("z.parquet"), PathBuf::from("a.parquet")];
    let expression = source_file_id_projection_expr(&paths);

    assert!(expression.contains("WHEN 'z.parquet' THEN 0::UINTEGER"));
    assert!(expression.contains("WHEN 'a.parquet' THEN 1::UINTEGER"));
    assert!(expression.contains("error("));
}

#[test]
fn core_rows_assign_unordered_dense_contract_ids() {
    let sql = build_core_rows_sql("'sample.parquet'");
    assert!(sql.contains("(row_number() OVER () - 1)::UINTEGER AS contract_id"));
    assert!(!sql.contains("ORDER BY chain, contract_address"));
}

#[test]
fn domain_projections_preserve_solana_case_only() {
    let projections = [
        build_core_rows_sql("'sample.parquet'"),
        build_uri_metadata_stream_sql(
            "'sample.parquet'",
            "metadata_json",
            "CAST(filename AS VARCHAR)",
        ),
    ];

    for sql in projections {
        assert!(sql.contains("WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'"));
        assert!(sql.contains("THEN trim(CAST(contract_address AS VARCHAR))"));
        assert!(sql.contains("ELSE lower(trim(CAST(contract_address AS VARCHAR)))"));
    }
}

#[test]
fn domain_projections_do_not_materialize_a_wide_analysis_table() {
    let core = build_core_rows_sql("'sample.parquet'");
    let stream = build_uri_metadata_stream_sql(
        "'sample.parquet'",
        "metadata_json",
        "CAST(filename AS VARCHAR)",
    );

    assert!(!core.contains("metadata_json"));
    assert!(!core.contains("token_uri_norm"));
    assert!(!stream.contains("name_norm"));
    assert!(!stream.to_ascii_uppercase().contains("CREATE TABLE"));
    assert!(!analysis_contracts_sql().contains("analysis_rows"));
    assert!(core.contains("CREATE OR REPLACE TEMP TABLE contract_dim"));
}

#[test]
fn uri_and_metadata_stream_share_one_parquet_scan_without_a_wide_table() {
    let sql = build_uri_metadata_stream_sql(
        "'sample.parquet'",
        "metadata_json",
        "CAST(filename AS VARCHAR)",
    );

    assert_eq!(sql.matches("read_parquet(").count(), 1);
    assert!(sql.contains("token_uri_norm"));
    assert!(sql.contains("image_uri_norm"));
    assert!(sql.contains("metadata_json"));
    assert!(!sql.to_ascii_uppercase().contains("CREATE TABLE"));
}

#[test]
fn metadata_rows_materialize_eligibility_once() {
    let sql = build_uri_metadata_stream_sql(
        "'sample.parquet'",
        "metadata_json",
        "CAST(filename AS VARCHAR)",
    );

    assert!(sql.contains("AS metadata_eligible"));
    assert!(analysis_contracts_sql().contains("WHERE metadata_eligible"));
    assert!(!analysis_contracts_sql().contains("FILTER (WHERE metadata_eligible)"));
}

#[test]
fn metadata_sql_eligibility_uses_utf8_bytes_not_characters() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let metadata = format!(r#"{{"description":"{}"}}"#, "界".repeat(22_000));
    assert!(metadata.chars().count() <= MAX_METADATA_BYTES_FOR_DEDUP);
    assert!(metadata.len() > MAX_METADATA_BYTES_FOR_DEDUP);
    let sql = format!("SELECT {}", metadata_json_eligible_predicate("?1"));

    let eligible = conn
        .query_row(&sql, [&metadata], |row| row.get::<_, bool>(0))
        .unwrap();

    assert!(!eligible);
}

#[test]
fn analysis_contracts_sql_selects_one_representative_per_contract() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
            CREATE TEMP TABLE source_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '1', '', '{"description":"first available"}'),
                ('ethereum', '0xaaa', '2', '', '{"description":"later repeated"}'),
                ('ethereum', '0xaaa', '3', '', '{"description":"later repeated"}'),
                ('ethereum', '0xbbb', '0', '', ''),
                ('ethereum', '0xbbb', '1', '', '{"description":"first usable for b"}'),
                ('ethereum', '0xbbb', '2', '', '{"description":"later b"}')
            ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
            CREATE TEMP TABLE contract_dim AS
            SELECT (row_number() OVER (ORDER BY chain, contract_address) - 1)::UINTEGER AS contract_id,
                   chain, contract_address, count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm
            FROM source_rows
            GROUP BY chain, contract_address;
            CREATE TEMP TABLE metadata_rows AS
            SELECT contracts.contract_id, token_id, metadata_json,
                   0::UINTEGER AS source_file,
                   row_number() OVER ()::UBIGINT AS source_row_number,
                   metadata_json <> '' AS metadata_eligible
            FROM source_rows
            JOIN contract_dim contracts USING (chain, contract_address);
        "#,
    )
    .unwrap();

    conn.execute_batch(&analysis_contracts_sql()).unwrap();
    let mut stmt = conn
        .prepare(
            "
            SELECT contracts.contract_address, rows.metadata_json, contracts.nft_count
            FROM analysis_contracts contracts
            JOIN metadata_rows rows
              ON rows.source_file = contracts.metadata_source_file
             AND rows.source_row_number = contracts.metadata_source_row_number
            ORDER BY contracts.contract_address
            ",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(contract, metadata, nft_count)| {
        contract == "0xaaa" && metadata == r#"{"description":"first available"}"# && *nft_count == 3
    }));
    assert!(rows.iter().any(|(contract, metadata, nft_count)| {
        contract == "0xbbb"
            && metadata == r#"{"description":"first usable for b"}"#
            && *nft_count == 3
    }));
}

#[test]
fn analysis_contracts_sql_uses_grouped_stable_representatives() {
    let sql = analysis_contracts_sql();

    assert!(!sql.contains("GROUP BY chain, contract_address, metadata_json"));
    assert!(sql.contains("GROUP BY contract_id"));
    assert!(sql.contains("arg_min("));
    assert!(sql.contains("row(token_id, source_file, source_row_number)"));
    assert!(sql.contains("WHERE metadata_eligible"));
    assert!(!sql.contains("metadata_contract_lookup"));
    assert!(!sql.contains("FULL OUTER JOIN"));
}

#[test]
fn analysis_contracts_aggregates_only_the_representative_row_id() {
    let sql = analysis_contracts_sql();

    assert_eq!(sql.matches("arg_min(").count(), 1);
    assert!(sql.contains("struct_pack("));
    assert!(sql.contains("metadata_source.file_id"));
    assert!(sql.contains("metadata_source.row_number"));
    assert!(sql.contains("indexed_metadata_sources"));
    assert!(sql.contains("row_number() OVER () - 1 AS metadata_contract_index"));
    assert!(!sql.contains("ORDER BY contract_id"));
    assert!(!sql.contains("count(*) FILTER"));
    assert!(!sql.contains("representative.metadata_json"));
    assert!(!sql.contains("JOIN metadata_rows representative"));
}

#[test]
fn output_generation_is_valid_only_after_manifest_publication() {
    let directory = tempfile::tempdir().unwrap();
    let report = output_generation_report("old", 2);
    fs::write(
        directory.path().join("summary.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    fs::write(directory.path().join("summary.csv"), b"old csv\n").unwrap();

    let before_publication = validate_output_generation(directory.path()).unwrap_err();
    assert!(before_publication
        .to_string()
        .contains("summary.manifest.json"));

    write_outputs(&report, directory.path()).unwrap();

    validate_output_generation(directory.path()).unwrap();
    assert!(directory.path().join("summary.manifest.json").is_file());
}

#[test]
fn output_generation_rejects_a_mixed_summary_pair() {
    let directory = tempfile::tempdir().unwrap();
    let first = output_generation_report("first", 2);
    write_outputs(&first, directory.path()).unwrap();
    validate_output_generation(directory.path()).unwrap();

    let second = output_generation_report("second", 3);
    fs::write(
        directory.path().join("summary.json"),
        serde_json::to_vec_pretty(&second).unwrap(),
    )
    .unwrap();

    let error = validate_output_generation(directory.path()).unwrap_err();
    assert!(error.to_string().contains("summary.json"));
}

#[test]
fn chain_totals_query_aggregates_all_chains_once() {
    let sql = chain_totals_sql();

    assert!(sql.contains("GROUP BY chain"));
    assert!(!sql.contains('?'));
}

#[test]
fn arrow_columns_preserve_utf8_integers_nulls_and_order() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let mut stmt = conn
        .prepare(
            "
            SELECT * FROM (
                VALUES
                    (0::BIGINT, '金色 dragon'::VARCHAR),
                    (1::BIGINT, NULL::VARCHAR)
            ) rows(row_index, text_value)
            ORDER BY row_index
            ",
        )
        .unwrap();
    let batches = stmt.query_arrow([]).unwrap().collect::<Vec<_>>();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let indexes = arrow_i64_column(batch, 0, "row_index").unwrap();
    let texts = arrow_string_column(batch, 1, "text_value").unwrap();
    assert_eq!(indexes.value(0), 0);
    assert_eq!(indexes.value(1), 1);
    assert_eq!(texts.value(0), "金色 dragon");
    assert!(duckdb::arrow::array::Array::is_null(texts, 1));
}

#[test]
fn analysis_contracts_sql_selects_lowest_token_id_representative() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
            CREATE TEMP TABLE source_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '2', '', '{"description":"rowid first"}'),
                ('ethereum', '0xaaa', '1', '', '{"description":"token id first"}')
            ) AS t(chain, contract_address, token_id, name_norm, metadata_json);
            CREATE TEMP TABLE contract_dim AS
            SELECT 0::UINTEGER AS contract_id,
                   chain, contract_address, count(*)::BIGINT AS nft_count,
                   min(nullif(name_norm, '')) AS name_norm
            FROM source_rows
            GROUP BY chain, contract_address;
            CREATE TEMP TABLE metadata_rows AS
            SELECT 0::UINTEGER AS contract_id, token_id, metadata_json,
                   0::UINTEGER AS source_file,
                   row_number() OVER ()::UBIGINT AS source_row_number,
                   metadata_json <> '' AS metadata_eligible
            FROM source_rows;
        "#,
    )
    .unwrap();

    conn.execute_batch(&analysis_contracts_sql()).unwrap();
    let metadata = conn
        .query_row(
            "SELECT rows.metadata_json
             FROM analysis_contracts contracts
             JOIN metadata_rows rows
               ON rows.source_file = contracts.metadata_source_file
              AND rows.source_row_number = contracts.metadata_source_row_number",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();

    assert_eq!(metadata, r#"{"description":"token id first"}"#);
}

#[test]
fn uri_duplicate_sql_skips_full_stats_tables() {
    let duplicate_sql = build_uri_duplicate_key_stats_sql();

    assert!(!duplicate_sql.contains("uri_key_stats"));
    assert!(duplicate_sql.contains("HAVING count(*) >= 2"));
    assert!(!duplicate_sql.contains("nft_count"));
}

#[test]
fn uri_key_contract_sql_aggregates_key_kinds_before_union() {
    let sql = build_uri_key_contracts_sql();

    assert_eq!(sql.matches("uri_rows").count(), 2);
    assert!(!sql.contains("CROSS JOIN LATERAL"));
    assert!(sql.contains("UNION ALL"));
    assert!(sql.contains("0::UTINYINT AS key_kind"));
    assert!(sql.contains("1::UTINYINT AS key_kind"));
    assert!(sql.contains("rows.chain_index"));
    assert!(!sql.contains("contract_dim"));
    assert!(!sql.contains("strict_token"));
    assert!(!sql.contains("nft_count"));
}

#[test]
fn single_chain_uri_flags_skip_cross_chain_key_tables() {
    let sql = build_uri_contract_flags_sql(false);

    assert!(!sql.contains("uri_key_chain_counts"));
    assert!(!sql.contains("uri_duplicate_key_chain_counts"));
    assert!(!sql.contains("uri_cross_chain_keys"));
    assert!(!sql.contains("norm_cross_chain"));
    assert!(!sql.contains("norm_token_chain"));
    assert!(!sql.contains("norm_image_chain"));
    assert!(sql.contains("norm_contract_v1_nfts"));
}

#[test]
fn multi_chain_uri_flags_include_cross_chain_tables_and_metrics() {
    let key_sql = build_uri_cross_chain_keys_sql();
    let flags_sql = build_uri_contract_flags_sql(true);
    let pair_sql = build_uri_chain_pair_contract_flags_sql();

    assert!(key_sql.contains("bit_count(chain_mask) >= 2"));
    assert!(!key_sql.contains("count(DISTINCT"));
    assert!(key_sql.contains("chain_mask"));
    assert!(!key_sql.contains("JOIN selected_chains"));
    assert!(flags_sql.contains("nt.chain_index = r.chain_index"));
    assert!(flags_sql.contains("nt.key_kind = 0"));
    assert!(flags_sql.contains("SELECT chain_index"));
    assert!(!flags_sql.contains("chains.chain"));
    assert!(flags_sql.contains("uri_cross_chain_keys"));
    assert!(flags_sql.contains("norm_cross_chain_v1_nfts"));
    assert!(pair_sql.contains("uri_cross_chain_keys"));
    assert!(pair_sql.contains("chain_mask"));
    assert!(pair_sql.contains("primary_chain"));
    assert!(pair_sql.contains("secondary_chain"));
    assert!(pair_sql.contains("norm_chain_v3_contracts"));
    assert!(pair_sql.contains("rowid AS uri_row_id"));
    assert!(pair_sql.contains("UNION ALL"));
    assert!(!pair_sql.contains("CROSS JOIN selected_chains"));
    assert!(pair_sql.contains("primary_chain_index"));
    assert!(pair_sql.contains("secondary_chain_index"));
    assert!(!pair_sql.contains("count(*)::BIGINT AS total_nfts"));
}

#[test]
fn cross_chain_uri_keys_use_compact_chain_masks() {
    let keys = build_uri_cross_chain_keys_sql();
    let flags = build_uri_contract_flags_sql(true);

    assert!(keys.contains("bit_or"));
    assert!(keys.contains("chain_mask"));
    assert!(flags.contains("chain_mask"));
    assert!(!flags.contains("uri_key_chain_presence"));
}

#[test]
fn chain_masks_reject_more_than_64_chains() {
    assert!(validate_chain_mask_capacity(64).is_ok());
    assert!(validate_chain_mask_capacity(65).is_err());
}

#[test]
fn chain_pair_count_query_aggregates_all_pairs_at_once() {
    let sql = uri_chain_pair_counts_sql();

    assert!(sql.contains("GROUP BY primary_chain_index, secondary_chain_index"));
    assert!(sql.contains("selected_chains"));
    assert!(!sql.contains('?'));
}

#[test]
fn contract_count_query_aggregates_all_chains_and_scopes_at_once() {
    let sql = uri_contract_counts_sql(true);

    assert!(sql.contains("GROUP BY chain"));
    assert!(sql.contains("selected_chains"));
    assert!(sql.contains("norm_contract_v1_nfts"));
    assert!(sql.contains("norm_cross_chain_v1_nfts"));
    assert!(!sql.contains('?'));
}
