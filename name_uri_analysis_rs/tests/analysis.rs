use std::path::Path;

use duckdb::Connection;
use name_uri_analysis_rs::analysis::{run_analysis, AnalysisOptions};

fn write_parquet(path: &Path, values_sql: &str) {
    let _ = std::fs::remove_file(path);
    let conn = Connection::open_in_memory().unwrap();
    let sql = format!(
        r#"
        CREATE TABLE rows AS
        SELECT * FROM ({values_sql}) AS t(chain, contract_address, token_id, token_uri, image_uri, name, name_norm);

        ALTER TABLE rows ADD COLUMN token_uri_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN image_uri_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN symbol VARCHAR;
        ALTER TABLE rows ADD COLUMN symbol_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN metadata_doc VARCHAR;
        UPDATE rows
        SET token_uri_norm = token_uri,
            image_uri_norm = image_uri,
            symbol = '',
            symbol_norm = '',
            metadata_doc = '';

        COPY rows TO '{path}' (FORMAT PARQUET);
        "#,
        path = path.display().to_string().replace('\\', "/"),
        values_sql = values_sql
    );
    conn.execute_batch(&sql).unwrap();
}

#[test]
fn persists_prepared_tables_when_requested() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    let db = temp.path().join("analysis.duckdb");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'shared', 'img1', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'shared', 'img2', 'Azuki', 'azuki')
        "#,
    );

    run_analysis(AnalysisOptions {
        database_path: db.clone(),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: true,
        reuse_prepared: false,
    })
    .unwrap();

    let conn = Connection::open(db).unwrap();
    for table in [
        "analysis_rows",
        "selected_chains",
        "uri_key_contracts",
        "uri_duplicate_key_stats",
        "uri_contract_flags",
        "contract_names",
        "name_atoms",
        "analysis_prepared_metadata",
    ] {
        let count: i64 = conn
            .query_row(
                "SELECT count(*)::BIGINT FROM information_schema.tables WHERE table_name = ?",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "missing persisted table {table}");
    }
}

#[test]
fn reuse_prepared_uses_persisted_tables_when_metadata_matches() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    let db = temp.path().join("analysis.duckdb");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'shared', 'img1', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'shared', 'img2', 'Azuki', 'azuki')
        "#,
    );

    run_analysis(AnalysisOptions {
        database_path: db.clone(),
        parquet_inputs: vec![parquet.clone()],
        output_dir: temp.path().join("out_first"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: true,
        reuse_prepared: false,
    })
    .unwrap();

    {
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "UPDATE name_atoms SET contract_count = 10, nft_count = 10 WHERE name_norm = 'azuki'",
            [],
        )
        .unwrap();
    }

    let report = run_analysis(AnalysisOptions {
        database_path: db,
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out_reuse"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: true,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "name"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.threshold == Some(90.0)
            && row.duplicate_contract_count == 10
            && row.duplicate_nft_count == 10
    }));
}

#[test]
fn reuse_prepared_rebuilds_when_cross_chain_cache_table_is_missing() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    let db = temp.path().join("analysis.duckdb");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'shared', 'img1', 'Azuki', 'azuki'),
            ('polygon', '0xbbb', '1', 'shared', 'img2', 'Azuki', 'azuki')
        "#,
    );

    run_analysis(AnalysisOptions {
        database_path: db.clone(),
        parquet_inputs: vec![parquet.clone()],
        output_dir: temp.path().join("out_first"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: true,
        reuse_prepared: false,
    })
    .unwrap();

    {
        let conn = Connection::open(&db).unwrap();
        conn.execute("DROP TABLE uri_duplicate_key_chain_counts", [])
            .unwrap();
    }

    run_analysis(AnalysisOptions {
        database_path: db.clone(),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out_rebuilt"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: true,
    })
    .unwrap();

    let conn = Connection::open(db).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT count(*)::BIGINT FROM information_schema.tables WHERE table_name = 'uri_duplicate_key_chain_counts'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn non_persistent_run_does_not_delete_persisted_tables() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    let db = temp.path().join("analysis.duckdb");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'shared', 'img1', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'shared', 'img2', 'Azuki', 'azuki')
        "#,
    );

    run_analysis(AnalysisOptions {
        database_path: db.clone(),
        parquet_inputs: vec![parquet.clone()],
        output_dir: temp.path().join("out_persist"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: true,
        reuse_prepared: false,
    })
    .unwrap();
    run_analysis(AnalysisOptions {
        database_path: db.clone(),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out_temp"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    let conn = Connection::open(db).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT count(*)::BIGINT FROM information_schema.tables WHERE table_name = 'analysis_prepared_metadata'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn analyzes_uri_and_name_without_symbol_rows() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    let db = temp.path().join("analysis.duckdb");
    let out = temp.path().join("out");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'ipfs://seed/1', 'https://img/1.png', 'Azuki #1', 'azuki'),
            ('ethereum', '0xbbb', '1', 'ipfs://seed/1', 'https://img/2.png', 'Azuki', 'azuki'),
            ('ethereum', '0xccc', '1', 'ipfs://seed/3', 'https://img/shared.png', 'Other Name', 'other name'),
            ('ethereum', '0xddd', '1', 'ipfs://seed/4', 'https://img/shared.png', 'Different', 'different'),
            ('base', '0xeee', '1', 'ipfs://seed/1', 'https://img/base.png', 'Azuki Mirror', 'azuki mirror'),
            ('base', '0xfff', '1', 'ipfs://base/2', 'https://img/base2.png', 'Unrelated', 'unrelated')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: db,
        parquet_inputs: vec![parquet],
        output_dir: out,
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(report
        .summary_rows
        .iter()
        .all(|row| row.field_name != "symbol"));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "strict_any"
            && row.metric == "v1"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 2
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "strict_any"
            && row.metric == "v2"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 2
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "name"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.threshold == Some(90.0)
            && row.duplicate_contract_count == 2
    }));
}

#[test]
fn analyzes_uri_rows_when_only_one_uri_field_is_present() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xtoken1', '1', 'ipfs://token-only', '', 'Token Only 1', 'token only 1'),
            ('ethereum', '0xtoken2', '1', 'ipfs://token-only', '', 'Token Only 2', 'token only 2'),
            ('ethereum', '0ximage1', '1', '', 'https://img/only.png', 'Image Only 1', 'image only 1'),
            ('ethereum', '0ximage2', '1', '', 'https://img/only.png', 'Image Only 2', 'image only 2')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "strict_any"
            && row.metric == "v1"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 2
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "strict_any"
            && row.metric == "v2"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 2
    }));
}

#[test]
fn uri_any_and_cross_contract_counts_stay_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'ipfs://same-contract', 'img-a1', 'A', 'a'),
            ('ethereum', '0xaaa', '2', 'ipfs://same-contract', 'img-a2', 'A', 'a'),
            ('ethereum', '0xbbb', '1', 'ipfs://unique-b', 'img-b1', 'B', 'b')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "strict_any"
            && row.metric == "v1"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 1
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "strict_cross"
            && row.metric == "v1"
            && row.duplicate_nft_count == 0
            && row.duplicate_contract_count == 0
    }));
}

#[test]
fn cross_chain_uri_counts_use_selected_chain_key_coverage() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'ipfs://shared-cross', 'img-eth', 'A', 'a'),
            ('base', '0xbbb', '1', 'ipfs://shared-cross', 'img-base', 'B', 'b'),
            ('polygon', '0xccc', '1', 'ipfs://polygon-only', 'img-poly', 'C', 'c')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "cross_chain_summary"
            && row.primary_chain == "ethereum"
            && row.match_mode == "strict"
            && row.metric == "v1"
            && row.duplicate_nft_count == 1
            && row.duplicate_contract_count == 1
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "cross_chain_summary"
            && row.primary_chain == "polygon"
            && row.match_mode == "strict"
            && row.metric == "v1"
            && row.duplicate_nft_count == 0
            && row.duplicate_contract_count == 0
    }));
}

#[test]
fn compares_names_across_former_block_boundaries() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'u2', 'i2', 'Bazuki', 'bazuki')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![88.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "name"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.threshold == Some(88.0)
            && row.duplicate_contract_count == 2
    }));
}

#[test]
fn repeated_nfts_in_one_contract_count_as_one_name_contract() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'Azuki', 'azuki'),
            ('ethereum', '0xaaa', '2', 'u2', 'i2', 'Azuki', 'azuki'),
            ('ethereum', '0xaaa', '3', 'u3', 'i3', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'u4', 'i4', 'Azuki', 'azuki')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    let row = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == "name"
                && row.scope == "intra_chain"
                && row.primary_chain == "ethereum"
                && row.threshold == Some(90.0)
        })
        .unwrap();

    assert_eq!(row.total_contracts, 2);
    assert_eq!(row.total_nfts, 4);
    assert_eq!(row.duplicate_contract_count, 2);
    assert_eq!(row.duplicate_nft_count, 4);
}

#[test]
fn contract_name_aggregation_keeps_empty_name_nfts_in_totals() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', '', ''),
            ('ethereum', '0xaaa', '2', 'u2', 'i2', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'u3', 'i3', 'Azuki', 'azuki')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    let row = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == "name"
                && row.scope == "intra_chain"
                && row.primary_chain == "ethereum"
                && row.threshold == Some(90.0)
        })
        .unwrap();

    assert_eq!(row.total_contracts, 2);
    assert_eq!(row.total_nfts, 3);
    assert_eq!(row.duplicate_contract_count, 2);
    assert_eq!(row.duplicate_nft_count, 3);
}

#[test]
fn only_parquet_chains_are_analyzed_and_single_chain_skips_cross_chain() {
    let temp = tempfile::tempdir().unwrap();
    let ethereum = temp.path().join("ethereum.parquet");
    write_parquet(
        &ethereum,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'shared', 'img1', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'unique', 'img2', 'Other', 'other')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![ethereum],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(report
        .summary_rows
        .iter()
        .all(|row| row.primary_chain == "ethereum"));
    assert!(report
        .summary_rows
        .iter()
        .all(|row| row.scope != "cross_chain_summary" && row.scope != "chain_matrix"));
}

#[test]
fn chain_matrix_is_computed_per_chain_pair_without_third_chain_contamination() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('base', '0xbase1', '1', 'u1', 'i1', 'Azuki', 'azuki'),
            ('ethereum', '0xeth1', '1', 'u2', 'i2', 'Azuki', 'azuki'),
            ('polygon', '0xpoly1', '1', 'u3', 'i3', 'Moonbirds', 'moonbirds')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![90.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "name"
            && row.scope == "chain_matrix"
            && row.primary_chain == "ethereum"
            && row.secondary_chain == "base"
            && row.threshold == Some(90.0)
            && row.duplicate_contract_count == 1
            && row.duplicate_nft_count == 1
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "name"
            && row.scope == "chain_matrix"
            && row.primary_chain == "ethereum"
            && row.secondary_chain == "polygon"
            && row.threshold == Some(90.0)
            && row.duplicate_contract_count == 0
            && row.duplicate_nft_count == 0
    }));
}

#[test]
fn batched_thresholds_match_single_threshold_results() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('base', '0xbase1', '1', 'u1', 'i1', 'Azuki', 'azuki'),
            ('base', '0xbase2', '1', 'u2', 'i2', 'Azuki Mirror', 'azuki mirror'),
            ('ethereum', '0xeth1', '1', 'u3', 'i3', 'Azuki', 'azuki'),
            ('ethereum', '0xeth2', '1', 'u4', 'i4', 'Azuki', 'azuki'),
            ('ethereum', '0xeth3', '1', 'u5', 'i5', 'Azzuki', 'azzuki'),
            ('polygon', '0xpoly1', '1', 'u6', 'i6', 'Moonbirds', 'moonbirds'),
            ('polygon', '0xpoly2', '1', 'u7', 'i7', 'Moonbirdz', 'moonbirdz')
        "#,
    );

    let batched = run_analysis(AnalysisOptions {
        database_path: temp.path().join("batched.duckdb"),
        parquet_inputs: vec![parquet.clone()],
        output_dir: temp.path().join("batched_out"),
        thresholds: vec![90.0, 95.0, 98.0],
        threads: 2,
        memory_limit: "512MB".into(),
        analysis_memory_limit: Some("128MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    let single_threshold = run_analysis(AnalysisOptions {
        database_path: temp.path().join("single.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("single_out"),
        thresholds: vec![90.0, 95.0, 98.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("1KB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert_eq!(batched.summary_rows, single_threshold.summary_rows);
}
