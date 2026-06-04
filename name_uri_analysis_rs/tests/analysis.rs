use std::path::{Path, PathBuf};

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
        ALTER TABLE rows ADD COLUMN metadata_json VARCHAR;
        ALTER TABLE rows ADD COLUMN metadata_doc VARCHAR;
        UPDATE rows
        SET token_uri_norm = token_uri,
            image_uri_norm = image_uri,
            symbol = '',
            symbol_norm = '',
            metadata_json = '',
            metadata_doc = '';

        COPY rows TO '{path}' (FORMAT PARQUET);
        "#,
        path = path.display().to_string().replace('\\', "/"),
        values_sql = values_sql
    );
    conn.execute_batch(&sql).unwrap();
}

fn write_parquet_with_metadata(path: &Path, values_sql: &str) {
    let _ = std::fs::remove_file(path);
    let conn = Connection::open_in_memory().unwrap();
    let sql = format!(
        r#"
        CREATE TABLE rows AS
        SELECT * FROM ({values_sql}) AS t(
            chain, contract_address, token_id, token_uri, image_uri, name, name_norm, metadata_json
        );

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
            metadata_doc = metadata_json;

        COPY rows TO '{path}' (FORMAT PARQUET);
        "#,
        path = path.display().to_string().replace('\\', "/"),
        values_sql = values_sql
    );
    conn.execute_batch(&sql).unwrap();
}

fn write_parquet_with_metadata_json_and_doc(path: &Path, values_sql: &str) {
    let _ = std::fs::remove_file(path);
    let conn = Connection::open_in_memory().unwrap();
    let sql = format!(
        r#"
        CREATE TABLE rows AS
        SELECT * FROM ({values_sql}) AS t(
            chain, contract_address, token_id, token_uri, image_uri, name, name_norm, metadata_json, metadata_doc
        );

        ALTER TABLE rows ADD COLUMN token_uri_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN image_uri_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN symbol VARCHAR;
        ALTER TABLE rows ADD COLUMN symbol_norm VARCHAR;
        UPDATE rows
        SET token_uri_norm = token_uri,
            image_uri_norm = image_uri,
            symbol = '',
            symbol_norm = '';

        COPY rows TO '{path}' (FORMAT PARQUET);
        "#,
        path = path.display().to_string().replace('\\', "/"),
        values_sql = values_sql
    );
    conn.execute_batch(&sql).unwrap();
}

#[test]
fn analyzes_with_duckdb_memory_database() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'shared', 'img1', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '1', 'shared', 'img2', 'Azuki', 'azuki')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![95.0],
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
            && row.duplicate_contract_count == 2
    }));
}

#[test]
fn duckdb_database_path_is_ignored_for_memory_mode() {
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
        thresholds: vec![95.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    assert!(!db.exists());
}

#[test]
fn analysis_does_not_persist_prepared_tables_when_requested() {
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
        thresholds: vec![95.0],
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
        assert_eq!(count, 0, "unexpected persisted table {table}");
    }
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
        thresholds: vec![95.0],
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
            && row.match_mode == "norm_cross"
            && row.metric == "v1"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 2
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "norm_cross"
            && row.metric == "v2"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 2
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "name"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.threshold == Some(95.0)
            && row.duplicate_contract_count == 2
    }));
    assert!(report.summary_rows.iter().all(|row| {
        row.field_name != "uri" || (row.scope == "intra_chain" && row.match_mode == "norm_cross")
    }));
    assert!(report
        .summary_rows
        .iter()
        .all(|row| row.field_name != "name" || row.threshold == Some(95.0)));
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
        thresholds: vec![95.0],
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
            && row.match_mode == "norm_cross"
            && row.metric == "v1"
            && row.duplicate_nft_count == 2
            && row.duplicate_contract_count == 2
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "uri"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.match_mode == "norm_cross"
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
        thresholds: vec![95.0],
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
            && row.match_mode == "norm_cross"
            && row.metric == "v1"
            && row.duplicate_nft_count == 0
            && row.duplicate_contract_count == 0
    }));
    assert!(report
        .summary_rows
        .iter()
        .all(|row| row.field_name != "uri" || row.match_mode == "norm_cross"));
}

#[test]
fn cross_chain_uri_rows_are_not_emitted() {
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
        thresholds: vec![95.0],
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
        .all(|row| row.field_name != "uri" || row.scope == "intra_chain"));
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
        thresholds: vec![95.0],
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
            && row.threshold == Some(95.0)
            && row.duplicate_contract_count == 0
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
        thresholds: vec![95.0],
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
                && row.threshold == Some(95.0)
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
        thresholds: vec![95.0],
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
                && row.threshold == Some(95.0)
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
        thresholds: vec![95.0],
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
        thresholds: vec![95.0],
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
            && row.threshold == Some(95.0)
            && row.duplicate_contract_count == 1
            && row.duplicate_nft_count == 1
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "name"
            && row.scope == "chain_matrix"
            && row.primary_chain == "ethereum"
            && row.secondary_chain == "polygon"
            && row.threshold == Some(95.0)
            && row.duplicate_contract_count == 0
            && row.duplicate_nft_count == 0
    }));
}

#[test]
fn metadata_analysis_uses_first_available_representatives_for_correctness() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{"description":"gold dragon rare background"}'),
            ('ethereum', '0xaaa', '2', 'u2', 'i2', 'A', 'a', '{"description":"x"}'),
            ('ethereum', '0xbbb', '1', 'u3', 'i3', 'B', 'b', '{"description":"gold dragon rare background"}'),
            ('ethereum', '0xccc', '1', 'u4', 'i4', 'C', 'c', '{"description":"silver cat"}'),
            ('base', '0xddd', '1', 'u5', 'i5', 'D', 'd', '{"description":"gold dragon rare background"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![95.0],
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
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.threshold == Some(0.6)
            && row.match_mode == "bm25_representative"
            && row.metric == "duplicate_group"
            && row.total_contracts == 3
            && row.total_nfts == 4
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 3
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "cross_chain_summary"
            && row.primary_chain == "base"
            && row.threshold == Some(0.6)
            && row.duplicate_contract_count == 1
            && row.duplicate_nft_count == 1
    }));
    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "chain_matrix"
            && row.primary_chain == "ethereum"
            && row.secondary_chain == "base"
            && row.threshold == Some(0.6)
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 3
    }));
}

#[test]
fn metadata_analysis_uses_first_available_metadata_doc_per_contract() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{"description":"shared alpha"}'),
            ('ethereum', '0xaaa', '2', 'u2', 'i2', 'A', 'a', '{"description":"long unrelated beta gamma delta epsilon zeta"}'),
            ('ethereum', '0xbbb', '1', 'u3', 'i3', 'B', 'b', '{"description":"shared alpha"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![95.0],
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
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.total_contracts == 2
            && row.total_nfts == 3
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 3
    }));
}

#[test]
fn metadata_analysis_does_not_match_same_schema_with_different_content_values() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{"name":"Alpha #1","image":"ipfs://alpha/1.png","attributes":[{"trait_type":"Background","value":"Blue"}]}'),
            ('ethereum', '0xbbb', '1', 'u2', 'i2', 'B', 'b', '{"name":"Beta #9","image":"ipfs://beta/9.png","attributes":[{"trait_type":"Background","value":"Red"}]}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![95.0],
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
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.total_contracts == 2
            && row.total_nfts == 2
            && row.duplicate_contract_count == 0
            && row.duplicate_nft_count == 0
    }));
}

#[test]
fn metadata_analysis_uses_metadata_doc_when_metadata_json_is_empty() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata_json_and_doc(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '', '{"description":"gold dragon"}'),
            ('ethereum', '0xbbb', '1', 'u2', 'i2', 'B', 'b', '', '{"description":"gold dragon"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![95.0],
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
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.total_contracts == 2
            && row.total_nfts == 2
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 2
    }));
}
