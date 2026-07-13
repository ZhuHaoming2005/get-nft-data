use super::*;
use sha2::{Digest, Sha256};

#[test]
fn authoritative_input_fingerprint_matches_ordered_serial_digest() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("z.parquet");
    let second = dir.path().join("a.parquet");
    std::fs::write(&first, b"first authoritative input").unwrap();
    std::fs::write(&second, b"second authoritative input").unwrap();
    let paths = vec![
        first.to_string_lossy().into_owned(),
        second.to_string_lossy().into_owned(),
    ];

    let fingerprint = DuckDbFeatureStore::fingerprint_authoritative_inputs(&paths).unwrap();
    let mut combined = Sha256::new();
    for path in &fingerprint.canonical_inputs {
        let bytes = std::fs::read(path).unwrap();
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        combined.update((path.len() as u64).to_le_bytes());
        combined.update(path.as_bytes());
        combined.update((bytes.len() as u64).to_le_bytes());
        combined.update(digest);
    }
    let expected = combined
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    assert_eq!(fingerprint.combined_sha256, expected);
}

#[test]
fn authoritative_import_cleans_up_temporary_relations() {
    let (_dir, store, _chains) = prepared_authoritative_store();
    let conn = store.conn().unwrap();
    let relation_count: i64 = conn
        .query_row(
            "SELECT count(*)
             FROM (
                 SELECT table_name AS relation_name FROM duckdb_tables()
                 UNION ALL
                 SELECT view_name AS relation_name FROM duckdb_views()
             ) relations
             WHERE relation_name IN (
                 'incoming_nft_features',
                 'deduped_incoming_nft_features'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(relation_count, 0);
}

#[test]
fn authoritative_import_resume_skips_committed_import_and_finishes_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("ethereum.parquet");
    let database = dir.path().join("features.duckdb");
    let parquet_sql = parquet
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    Connection::open_in_memory()
        .unwrap()
        .execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES
              ('ethereum', '0xabc', '1', 'ipfs://one', '', 'One', 'ONE', '{{\"kind\":\"one\"}}', 'ipfs:one', '', 'one')
            ) AS t(chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm))
            TO '{parquet_sql}' (FORMAT PARQUET)"
        ))
        .unwrap();
    let store = DuckDbFeatureStore::new(database.to_str().unwrap()).unwrap();
    let paths = vec![parquet.to_string_lossy().into_owned()];

    let chains = store
        .import_authoritative_parquet_snapshot(&paths, false)
        .unwrap();
    let first_generation = store.snapshot_identity("ethereum").unwrap();
    assert_eq!(
        store.prepare_journal_phase().unwrap().as_deref(),
        Some("imported")
    );

    let resumed_chains = store
        .import_authoritative_parquet_snapshot(&paths, false)
        .unwrap();
    let resumed_generation = store.snapshot_identity("ethereum").unwrap();

    assert_eq!(resumed_chains, chains);
    assert_eq!(resumed_generation, first_generation);

    store.prepare_recall_for_chains(&resumed_chains).unwrap();

    assert_eq!(
        store.prepare_journal_phase().unwrap().as_deref(),
        Some("ready")
    );
    store.require_prepared_for_chains(&chains).unwrap();
}

#[test]
fn authoritative_input_validation_detects_changes_after_initial_fingerprint() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("snapshot.parquet");
    std::fs::write(&input, b"initial authoritative bytes").unwrap();
    let fingerprint = DuckDbFeatureStore::fingerprint_authoritative_inputs(&[input
        .to_string_lossy()
        .into_owned()])
    .unwrap();

    std::fs::write(&input, b"changed authoritative bytes").unwrap();

    let error =
        DuckDbFeatureStore::validate_authoritative_inputs_unchanged(&fingerprint).unwrap_err();
    assert!(error.to_string().contains("changed"), "{error}");
}

#[test]
fn failed_authoritative_prepare_persists_the_primary_error_in_the_journal() {
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("ethereum.parquet");
    let database = dir.path().join("features.duckdb");
    let parquet_sql = parquet
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");
    Connection::open_in_memory()
        .unwrap()
        .execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES
              ('ethereum', '0xabc', '1', 'ipfs://one', '', 'One', 'ONE', '{{\"kind\":\"one\"}}', 'ipfs:one', '', 'one')
            ) AS t(chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm))
            TO '{parquet_sql}' (FORMAT PARQUET)"
        ))
        .unwrap();
    let store = DuckDbFeatureStore::new(database.to_str().unwrap()).unwrap();
    let chains = store
        .import_authoritative_parquet_snapshot(&[parquet.to_string_lossy().into_owned()], false)
        .unwrap();
    store
        .conn()
        .unwrap()
        .execute(
            "UPDATE nft_feature_generations SET generation_id = 'corrupted-generation'",
            [],
        )
        .unwrap();

    let error = store.prepare_recall_for_chains(&chains).unwrap_err();
    let last_error: String = store
        .conn()
        .unwrap()
        .query_row(
            "SELECT last_error FROM nft_prepare_journal WHERE journal_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert!(
        !last_error.is_empty(),
        "primary error was not journaled: {error}"
    );
    assert!(last_error.contains("generation"), "{last_error}");
}

#[test]
fn arrow_columns_preserve_utf8_integers_nulls_and_order() {
    let conn = Connection::open_in_memory().unwrap();
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

    let batch = &batches[0];
    let indexes = arrow_i64_column(batch, 0, "row_index").unwrap();
    let texts = arrow_string_column(batch, 1, "text_value").unwrap();
    assert_eq!(indexes.value(0), 0);
    assert_eq!(indexes.value(1), 1);
    assert_eq!(texts.value(0), "金色 dragon");
    assert!(duckdb::arrow::array::Array::is_null(texts, 1));
}

#[test]
fn feature_store_configures_bulk_import_checkpoint_limits() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    let conn = store.conn().unwrap();
    let checkpoint_threshold: String = conn
        .query_row(
            "SELECT current_setting('checkpoint_threshold')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let skip_wal_threshold: u64 = conn
        .query_row(
            "SELECT current_setting('auto_checkpoint_skip_wal_threshold')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let write_buffer_row_group_count: u64 = conn
        .query_row(
            "SELECT current_setting('write_buffer_row_group_count')",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert!(checkpoint_threshold.contains("GiB"));
    assert_eq!(skip_wal_threshold, BULK_IMPORT_SKIP_WAL_THRESHOLD_BYTES);
    assert_eq!(write_buffer_row_group_count, 1);
}
