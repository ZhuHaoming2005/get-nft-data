use std::path::{Path, PathBuf};

use duckdb::Connection;
use name_uri_analysis_rs::analysis::{
    finalize_analysis_phases, run_analysis, run_analysis_phase, AnalysisOptions, AnalysisPhase,
    AnalysisReport,
};

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

#[test]
fn isolated_process_phases_match_the_in_process_report() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
        VALUES
            ('ethereum', '0xaaa', '1', 'shared', 'img1', 'Azuki', 'azuki'),
            ('ethereum', '0xbbb', '2', 'shared', 'img2', 'Azuki Clone', 'azuki clone'),
            ('polygon', '0xccc', '1', 'other', 'img1', 'Azuki', 'azuki')
        "#,
    );

    let phase_work = temp.path().join("phase-work");
    let phase_options = AnalysisOptions {
        database_path: phase_work.join("stage.duckdb"),
        parquet_inputs: vec![parquet.clone()],
        output_dir: temp.path().join("phase-output"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: Some(phase_work.join("duckdb-temp")),
        progress: false,
    };
    for phase in [
        AnalysisPhase::Prepare,
        AnalysisPhase::MetadataEncode,
        AnalysisPhase::Name,
        AnalysisPhase::MetadataMatch,
    ] {
        run_analysis_phase(&phase_options, phase, &phase_work).unwrap();
    }
    assert!(phase_work.join("partial/metadata-summary.json").is_file());
    assert!(phase_work
        .join("artifacts/metadata/match-1/metadata-summary-1/metadata-summary.ready")
        .is_file());
    let phased = finalize_analysis_phases(&phase_options, &phase_work).unwrap();

    let mut in_process_options = phase_options.clone();
    in_process_options.database_path = temp.path().join("in-process.duckdb");
    in_process_options.output_dir = temp.path().join("in-process-output");
    in_process_options.temp_directory = None;
    let in_process = run_analysis(in_process_options).unwrap();

    assert_eq!(phased, in_process);
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

#[test]
fn metadata_phase_continues_when_measured_state_exceeds_the_accounting_budget() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("metadata-budget.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
        VALUES
            ('ethereum', '0xaaa', '1', '', '', 'A', 'a', '{"description":"shared"}'),
            ('ethereum', '0xbbb', '1', '', '', 'B', 'b', '{"description":"shared"}')
        "#,
    );
    let work = temp.path().join("work");
    let options = AnalysisOptions {
        database_path: work.join("stage.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("output"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("1B".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: Some(work.join("duckdb-temp")),
        progress: false,
    };
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
    run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();

    run_analysis_phase(&options, AnalysisPhase::MetadataMatch, &work).unwrap();
    assert!(work.join("partial/metadata-summary.json").is_file());
    assert!(work
        .join("artifacts/metadata/match-1/metadata-summary-1/metadata-summary.ready")
        .is_file());
}

#[test]
fn completed_name_result_does_not_depend_on_destructive_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let database = work.join("stage.duckdb");
    let conn = Connection::open(&database).unwrap();
    conn.execute_batch(
        "CREATE TABLE selected_chains AS SELECT 'ethereum'::VARCHAR AS chain, 0::UINTEGER AS chain_index;
         CREATE TABLE analysis_contracts AS
         SELECT 0::UINTEGER AS contract_id, 'ethereum'::VARCHAR AS chain,
                '0xaaa'::VARCHAR AS contract_address, 1::BIGINT AS nft_count,
                'azuki'::VARCHAR AS name_norm,
                NULL::UINTEGER AS metadata_source_file,
                NULL::UBIGINT AS metadata_source_row_number,
                NULL::BIGINT AS metadata_contract_index;
         CREATE TABLE chain_totals AS
         SELECT 'ethereum'::VARCHAR AS chain, 1::BIGINT AS contract_count,
                1::BIGINT AS nft_count;
         CREATE VIEW name_atoms AS
         SELECT 0::BIGINT AS atom_id, 'ethereum'::VARCHAR AS chain,
                'azuki'::VARCHAR AS name_norm, 1::BIGINT AS contract_count,
                1::BIGINT AS nft_count;",
    )
    .unwrap();
    drop(conn);
    let options = AnalysisOptions {
        database_path: database,
        parquet_inputs: vec![temp.path().join("unused.parquet")],
        output_dir: temp.path().join("output"),
        name_threshold: 95.0,
        threads: 1,
        memory_limit: "64MiB".into(),
        analysis_memory_limit: Some("64MiB".into()),
        duckdb_memory_limit: "64MiB".into(),
        temp_directory: Some(work.join("duckdb-temp")),
        progress: false,
    };

    run_analysis_phase(&options, AnalysisPhase::Name, &work).unwrap();

    assert!(work.join("partial/name-summary.json").is_file());
    assert!(
        work.join("checkpoints/name.ready.json").is_file(),
        "a durable result must be promotable before controller manifest update"
    );
    let conn = Connection::open(&options.database_path).unwrap();
    let view_exists = conn
        .query_row(
            "SELECT count(*) > 0 FROM duckdb_views() WHERE view_name = 'name_atoms'",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap();
    assert!(
        view_exists,
        "phase completion must not mutate staging inputs"
    );
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

fn assert_uri_row(
    report: &AnalysisReport,
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    metric: &str,
    duplicate_nfts: i64,
    duplicate_contracts: i64,
) {
    let row = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == "uri"
                && row.scope == scope
                && row.primary_chain == primary_chain
                && row.secondary_chain == secondary_chain
                && row.metric == metric
        })
        .expect("expected URI summary row");
    assert_eq!(row.duplicate_nft_count, duplicate_nfts);
    assert_eq!(row.duplicate_contract_count, duplicate_contracts);
}

#[allow(clippy::too_many_arguments)]
fn assert_nft_scope(
    report: &AnalysisReport,
    field_name: &str,
    metric: &str,
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    total_nfts: i64,
    duplicate_nfts: i64,
    duplicate_ratio: f64,
) {
    let row = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == field_name
                && row.metric == metric
                && row.scope == scope
                && row.primary_chain == primary_chain
                && row.secondary_chain == secondary_chain
        })
        .expect("expected three-scope summary row");
    assert_eq!(row.total_nfts, total_nfts);
    assert_eq!(row.duplicate_nft_count, duplicate_nfts);
    assert!((row.duplicate_nft_ratio - duplicate_ratio).abs() < 1e-9);
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
fn compatibility_entry_keeps_staging_out_of_the_caller_database_path() {
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(!db.exists());
    assert!(temp.path().join("out/summary.manifest.json").is_file());
    assert!(!std::fs::read_dir(temp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("metadata-work")
    }));
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
        row.field_name != "uri"
            || (matches!(
                row.scope.as_str(),
                "intra_chain" | "cross_chain_summary" | "chain_matrix"
            ) && row.match_mode == "norm_cross")
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
fn cross_chain_uri_rows_emit_summary_and_isolated_pair_matrix() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xeth-token', '1', 'shared-token', 'shared-token-image', 'A', 'a'),
            ('base', '0xbase-token', '1', 'shared-token', 'shared-token-image', 'B', 'b'),
            ('ethereum', '0xeth-image', '2', 'eth-only', 'shared-image', 'C', 'c'),
            ('polygon', '0xpoly-image', '1', 'poly-only', 'shared-image', 'D', 'd'),
            ('solana', 'So11111111111111111111111111111111111111112',
             'Mint111111111111111111111111111111111111111', 'sol-only', 'sol-image', 'E', 'e')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert_uri_row(&report, "cross_chain_summary", "ethereum", "", "v1", 1, 1);
    assert_uri_row(&report, "cross_chain_summary", "ethereum", "", "v2", 1, 1);
    assert_uri_row(&report, "cross_chain_summary", "ethereum", "", "v3", 2, 2);

    assert_uri_row(&report, "chain_matrix", "ethereum", "base", "v1", 1, 1);
    assert_uri_row(&report, "chain_matrix", "ethereum", "base", "v2", 0, 0);
    assert_uri_row(&report, "chain_matrix", "ethereum", "polygon", "v2", 1, 1);
    assert_uri_row(&report, "chain_matrix", "ethereum", "solana", "v3", 0, 0);
    assert_uri_row(&report, "chain_matrix", "base", "polygon", "v3", 0, 0);
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
fn name_pairwise_matching_does_not_apply_transitive_closure() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'abcdefghij', 'abcdefghij'),
            ('ethereum', '0xbbb', '1', 'u2', 'i2', 'abcdefghiX', 'abcdefghix'),
            ('ethereum', '0xccc', '1', 'u3', 'i3', 'abcdefghXX', 'abcdefghxx')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    let row = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == "name"
                && row.scope == "intra_chain"
                && row.primary_chain == "ethereum"
        })
        .unwrap();
    assert_eq!(row.match_mode, "jaro_winkler_pairwise");
    assert_eq!(row.metric, "duplicate_pair");
    assert_eq!(row.group_count, 2);
    assert_eq!(row.duplicate_contract_count, 3);
    assert_eq!(row.group_size_ge_2_count, 2);
    assert_eq!(row.group_size_gt_2_count, 0);
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
fn metadata_analysis_uses_deterministic_representatives_for_correctness() {
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.threshold == Some(0.6)
            && row.match_mode == "template_recall_hybrid_verify"
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
fn four_chain_analysis_uses_chain_aware_contract_identity() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("four-chains.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xAbC', '1', 'eth-1', 'eth-img-1', 'Eth One', 'eth one'),
            ('ethereum', '0xabc', '2', 'eth-2', 'eth-img-2', 'Eth Two', 'eth two'),
            ('base', '0xBase', '1', 'base-1', 'base-img-1', 'Base', 'base'),
            ('polygon', '0xPoly', '1', 'poly-1', 'poly-img-1', 'Polygon', 'polygon'),
            ('solana', 'Abc111111111111111111111111111111111111111', 'Mint1', 'sol-1', 'sol-img-1', 'Sol One', 'sol one'),
            ('solana', 'abc111111111111111111111111111111111111111', 'Mint2', 'sol-2', 'sol-img-2', 'Sol Two', 'sol two')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    let ethereum = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == "name"
                && row.scope == "intra_chain"
                && row.primary_chain == "ethereum"
        })
        .unwrap();
    let solana = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == "name" && row.scope == "intra_chain" && row.primary_chain == "solana"
        })
        .unwrap();

    assert_eq!(ethereum.total_contracts, 1);
    assert_eq!(solana.total_contracts, 2);
    assert!(report
        .summary_rows
        .iter()
        .any(|row| row.scope == "chain_matrix"));
}

#[test]
fn metadata_analysis_uses_lowest_token_metadata_per_contract() {
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
fn metadata_analysis_skips_empty_lowest_token_metadata_representative() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{}'),
            ('ethereum', '0xaaa', '2', 'u2', 'i2', 'A', 'a', '{"description":"shared alpha"}'),
            ('ethereum', '0xbbb', '3', 'u3', 'i3', 'B', 'b', '{"description":"shared alpha"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 3
    }));
}

#[test]
fn metadata_analysis_fallback_picks_lowest_token_id_survivor_deterministically() {
    // Contract 0xaaa has three metadata rows: token_id=1 (`{}`, which yields no
    // BM25 document and is skipped), plus two survivors with *different*
    // content (token_id=2 "shared alpha", token_id=3 "completely different
    // zeta"). The arg_min representative is token_id=1, so 0xaaa is resolved via
    // the fallback path, which must deterministically keep the lowest-(token_id,
    // stable SourceId) survivor — token_id=2 — and therefore match 0xbbb ("shared alpha").
    // A non-deterministic fallback that kept token_id=3 would instead produce
    // zero duplicates.
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{}'),
            ('ethereum', '0xaaa', '2', 'u2', 'i2', 'A', 'a', '{"description":"shared alpha"}'),
            ('ethereum', '0xaaa', '3', 'u3', 'i3', 'A', 'a', '{"description":"completely different zeta"}'),
            ('ethereum', '0xbbb', '4', 'u4', 'i4', 'B', 'b', '{"description":"shared alpha"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 4
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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
fn metadata_analysis_accepts_any_matching_overlapping_token_id() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{"name":"Alpha","image":"ipfs://alpha"}'),
            ('ethereum', '0xaaa', '2', 'u2', 'i2', 'A', 'a', '{"description":"gold dragon"}'),
            ('ethereum', '0xbbb', '1', 'u3', 'i3', 'B', 'b', '{"name":"Beta","image":"ar://beta"}'),
            ('ethereum', '0xbbb', '2', 'u4', 'i4', 'B', 'b', '{"description":"gold dragon"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 4
    }));
}

#[test]
fn metadata_analysis_skips_empty_lowest_shared_token_source() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '0', 'u0a', 'i0a', 'A', 'a', '{"description":"alpha only"}'),
            ('ethereum', '0xaaa', '1', 'u1a', 'i1a', 'A', 'a', '{}'),
            ('ethereum', '0xaaa', '1', 'u1b', 'i1b', 'A', 'a', '{"description":"shared gold"}'),
            ('ethereum', '0xbbb', '0', 'u0b', 'i0b', 'B', 'b', '{"description":"beta only"}'),
            ('ethereum', '0xbbb', '1', 'u1c', 'i1c', 'B', 'b', '{"description":"shared gold"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 5
    }));
}

#[test]
fn metadata_analysis_falls_back_to_representatives_without_common_token_id() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{"description":"gold dragon"}'),
            ('solana', 'Collection111', 'Mint111', 'u2', 'i2', 'B', 'b', '{"description":"gold dragon"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "chain_matrix"
            && row.primary_chain == "ethereum"
            && row.secondary_chain == "solana"
            && row.match_mode == "template_recall_hybrid_verify"
            && row.duplicate_contract_count == 1
            && row.duplicate_nft_count == 1
    }));
}

#[test]
fn summary_rows_use_chain_totals_as_common_denominators() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'shared-uri', 'img1', 'Azuki', 'azuki', '{"description":"gold dragon alpha"}'),
            ('ethereum', '0xbbb', '1', 'shared-uri', 'img2', 'Azuki', 'azuki', '{"description":"gold dragon alpha"}'),
            ('ethereum', '0xccc', '1', '', '', '', '', ''),
            ('ethereum', '0xccc', '2', '', '', '', '', '')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    for (field_name, metric) in [
        ("metadata", "duplicate_group"),
        ("name", "duplicate_pair"),
        ("uri", "v1"),
    ] {
        let row = report
            .summary_rows
            .iter()
            .find(|row| {
                row.field_name == field_name
                    && row.scope == "intra_chain"
                    && row.primary_chain == "ethereum"
                    && row.metric == metric
            })
            .unwrap();

        assert_eq!(row.total_contracts, 3, "{field_name}");
        assert_eq!(row.total_nfts, 4, "{field_name}");
        assert_eq!(row.duplicate_contract_count, 2, "{field_name}");
        assert_eq!(row.duplicate_nft_count, 2, "{field_name}");
        assert_eq!(row.duplicate_contract_ratio, 200.0 / 3.0, "{field_name}");
        assert_eq!(row.duplicate_nft_ratio, 50.0, "{field_name}");
    }
}

#[test]
fn metadata_analysis_uses_template_recall_before_content_verification() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xaaa', '1', 'u1', 'i1', 'A', 'a', '{"description":"gold dragon rare background shine","image":"ipfs://alpha/1.png"}'),
            ('ethereum', '0xbbb', '1', 'u2', 'i2', 'B', 'b', '{"description":"gold dragon rare background shine","image":"ipfs://alpha/2.png"}'),
            ('ethereum', '0xccc', '1', 'u3', 'i3', 'C', 'c', '{"description":"gold dragon rare background shine","image":"ipfs://beta/1.png"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    assert!(report.summary_rows.iter().any(|row| {
        row.field_name == "metadata"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
            && row.total_contracts == 3
            && row.total_nfts == 3
            && row.duplicate_contract_count == 2
            && row.duplicate_nft_count == 2
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
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
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

#[test]
fn three_scope_reporting_keeps_pools_isolated_and_uses_primary_chain_totals() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sample.parquet");
    write_parquet_with_metadata(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xeth-token-cross', '1', 'shared-token-cross', 'img-eth-token-cross',
             'TokenCross', 'tokencross', '{"description":"token cross alpha"}'),
            ('base', '0xbase-token-cross', '1', 'shared-token-cross', 'img-base-token-cross',
             'TokenCross', 'tokencross', '{"description":"token cross alpha"}'),
            ('polygon', '0xpoly-token-cross', '1', 'shared-token-cross', 'img-poly-token-cross',
             'TokenCross', 'tokencross', '{"description":"token cross alpha"}'),
            ('ethereum', '0xeth-image-cross', '1', 'token-eth-image-cross', 'shared-image-cross',
             'ImageCross', 'imagecross', '{"description":"image cross beta"}'),
            ('base', '0xbase-image-cross', '1', 'token-base-image-cross', 'shared-image-cross',
             'ImageCross', 'imagecross', '{"description":"image cross beta"}'),
            ('polygon', '0xpoly-image-cross', '1', 'token-poly-image-cross', 'shared-image-cross',
             'ImageCross', 'imagecross', '{"description":"image cross beta"}'),
            ('ethereum', '0xeth-token-intra-a', '1', 'eth-token-intra', 'img-eth-token-intra-a',
             'TokenIntra', 'tokenintra', '{"description":"ethereum token intra gamma"}'),
            ('ethereum', '0xeth-token-intra-b', '1', 'eth-token-intra', 'img-eth-token-intra-b',
             'TokenIntra', 'tokenintra', '{"description":"ethereum token intra gamma"}'),
            ('ethereum', '0xeth-image-intra-a', '1', 'token-eth-image-intra-a', 'eth-image-intra',
             'ImageIntra', 'imageintra', '{"description":"ethereum image intra delta"}'),
            ('ethereum', '0xeth-image-intra-b', '1', 'token-eth-image-intra-b', 'eth-image-intra',
             'ImageIntra', 'imageintra', '{"description":"ethereum image intra delta"}'),
            ('solana', 'SolOnly111', 'MintOnly111', 'sol-only', 'img-sol-only',
             'SolOnly', 'solonly', '{"description":"solana only beta"}')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: temp.path().join("analysis.duckdb"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: None,
        progress: false,
    })
    .unwrap();

    for (field, metric, cross_duplicate_nfts, intra_duplicate_nfts) in [
        ("name", "duplicate_pair", 2, 4),
        ("uri", "v1", 1, 2),
        ("uri", "v2", 1, 2),
        ("uri", "v3", 2, 4),
        ("metadata", "duplicate_group", 2, 4),
    ] {
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "ethereum",
            "base",
            6,
            cross_duplicate_nfts,
            cross_duplicate_nfts as f64 * 100.0 / 6.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "base",
            "ethereum",
            2,
            cross_duplicate_nfts,
            cross_duplicate_nfts as f64 * 100.0 / 2.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "base",
            "polygon",
            2,
            cross_duplicate_nfts,
            cross_duplicate_nfts as f64 * 100.0 / 2.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "polygon",
            "base",
            2,
            cross_duplicate_nfts,
            cross_duplicate_nfts as f64 * 100.0 / 2.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "ethereum",
            "polygon",
            6,
            cross_duplicate_nfts,
            cross_duplicate_nfts as f64 * 100.0 / 6.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "polygon",
            "ethereum",
            2,
            cross_duplicate_nfts,
            cross_duplicate_nfts as f64 * 100.0 / 2.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "ethereum",
            "solana",
            6,
            0,
            0.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "chain_matrix",
            "solana",
            "ethereum",
            1,
            0,
            0.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "cross_chain_summary",
            "ethereum",
            "",
            6,
            cross_duplicate_nfts,
            cross_duplicate_nfts as f64 * 100.0 / 6.0,
        );
        assert_nft_scope(
            &report,
            field,
            metric,
            "intra_chain",
            "ethereum",
            "",
            6,
            intra_duplicate_nfts,
            intra_duplicate_nfts as f64 * 100.0 / 6.0,
        );
    }
}
