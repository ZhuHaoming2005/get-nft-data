use super::*;

pub(super) fn prepared_authoritative_store() -> (
    tempfile::TempDir,
    DuckDbFeatureStore,
    Vec<crate::models::Chain>,
) {
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
    store.prepare_recall_for_chains(&chains).unwrap();
    (dir, store, chains)
}
