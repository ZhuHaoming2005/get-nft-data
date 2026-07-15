use std::fs;
use std::path::Path;

use duckdb::Connection;
use sha2::{Digest, Sha256};

use crate::analysis::{
    complete_metadata_payload_independence, run_analysis_phase, AnalysisOptions, AnalysisPhase,
};
use metadata_engine::blocking::BLOCKING_REVISION;
use metadata_engine::encode::{EncodeBundle, ENCODE_SCHEMA_REVISION};
use metadata_engine::resource::{MemoryBroker, GIB};
use metadata_engine::storage::StorageBroker;

use super::super::encode::{
    estimate_encode_storage_bytes, open_prepare_for_encode, resolve_fallback_contracts,
    stream_encode_inputs_with_progress,
};

#[test]
fn fallback_resolution_parses_each_contract_only_until_its_first_usable_row() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let rows = vec![
        (1, "{}".to_string(), 0, 0),
        (1, r#"{"description":"first"}"#.to_string(), 0, 1),
        (1, r#"{"description":"unused"}"#.to_string(), 0, 2),
        (2, r#"{"description":"second"}"#.to_string(), 0, 3),
    ];
    let calls = AtomicUsize::new(0);

    let resolved = resolve_fallback_contracts(&rows, &|json| {
        calls.fetch_add(1, Ordering::Relaxed);
        metadata_engine::encode::parse_metadata_documents(json)
    });

    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].0 .0, 1);
    assert_eq!(resolved[0].0 .1, r#"{"description":"first"}"#);
    assert_eq!(resolved[1].0 .0, 2);
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

fn write_tiny_metadata_parquet(path: &Path) {
    let _ = fs::remove_file(path);
    let conn = Connection::open_in_memory().unwrap();
    let sql = format!(
        r#"
        CREATE TABLE rows AS
        SELECT * FROM (VALUES
            ('ethereum', '0xaaa', '1', '', '', 'Azuki', 'azuki', '{{"description":"shared gold"}}'),
            ('ethereum', '0xbbb', '1', '', '', 'Azuki Clone', 'azuki clone', '{{"description":"shared gold"}}'),
            ('base', '0xccc', '1', '', '', 'Azuki', 'azuki', '{{"description":"shared gold"}}')
        ) AS t(chain, contract_address, token_id, token_uri, image_uri, name, name_norm, metadata_json);

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
        path = path.display().to_string().replace('\\', "/")
    );
    conn.execute_batch(&sql).unwrap();
}

fn tiny_options(work: &Path, parquet: &Path) -> AnalysisOptions {
    AnalysisOptions {
        database_path: work.join("stage.duckdb"),
        parquet_inputs: vec![parquet.to_path_buf()],
        output_dir: work.join("output"),
        name_threshold: 95.0,
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        duckdb_memory_limit: "256MB".into(),
        temp_directory: Some(work.join("duckdb-temp")),
        progress: false,
    }
}

fn file_sha256(path: &Path) -> [u8; 32] {
    let bytes = fs::read(path).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    hasher.finalize().into()
}

fn artifact_tree_fingerprints(root: &Path) -> std::collections::BTreeMap<String, [u8; 32]> {
    fn collect(root: &Path, directory: &Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(root, &path, files);
            } else {
                files.push(path.strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }
    let mut files = Vec::new();
    collect(root, root, &mut files);
    files.sort_unstable();
    let mut output = std::collections::BTreeMap::new();
    for relative in files {
        output.insert(
            relative.to_string_lossy().replace('\\', "/"),
            file_sha256(&root.join(relative)),
        );
    }
    output
}

fn prepare_identity_fingerprint(database: &Path) -> String {
    let conn = Connection::open(database).unwrap();
    conn.query_row(
        "SELECT string_agg(
             cast(contract_id AS VARCHAR) || ':' || chain || ':' || contract_address || ':' ||
             coalesce(cast(metadata_contract_index AS VARCHAR), 'none'),
             '|' ORDER BY chain, contract_address)
         FROM analysis_contracts",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn prepare_dense_identities_are_deterministic_across_thread_counts() {
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let mut identities = Vec::new();
    for threads in [1, 4] {
        let work = dir.path().join(format!("prepare-{threads}"));
        fs::create_dir_all(&work).unwrap();
        let mut options = tiny_options(&work, &parquet);
        options.threads = threads;
        run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
        identities.push(prepare_identity_fingerprint(&options.database_path));
    }
    assert_eq!(identities[0], identities[1]);
}

#[test]
fn parallel_encode_is_byte_deterministic_across_thread_counts() {
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let prepared = dir.path().join("prepared");
    fs::create_dir_all(&prepared).unwrap();
    let prepared_options = tiny_options(&prepared, &parquet);
    run_analysis_phase(&prepared_options, AnalysisPhase::Prepare, &prepared).unwrap();
    let mut digests = Vec::new();
    for threads in [1, 4] {
        let work = dir.path().join(format!("work-{threads}"));
        fs::create_dir_all(&work).unwrap();
        let mut options = tiny_options(&work, &parquet);
        options.threads = threads;
        fs::copy(&prepared_options.database_path, &options.database_path).unwrap();
        run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();
        digests.push(artifact_tree_fingerprints(&work.join("artifacts/metadata")));
    }
    assert_eq!(digests[0], digests[1]);
}

#[test]
fn encode_admission_freezes_row_totals_for_progress_before_streaming() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let options = tiny_options(&work, &parquet);
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
    let conn = Connection::open(&options.database_path).unwrap();

    let estimate = estimate_encode_storage_bytes(&conn).unwrap();
    let representative_rows: u64 = conn
        .query_row(
            "SELECT count(*)::UBIGINT FROM analysis_contracts WHERE metadata_contract_index IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let token_rows: u64 = conn
        .query_row(
            "SELECT count(*)::UBIGINT FROM metadata_contract_token_rows",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(estimate.representative_rows, representative_rows);
    assert_eq!(estimate.token_rows, token_rows);
    let token_json_bytes: u64 = conn
        .query_row(
            "SELECT coalesce(sum(length(metadata_rows.metadata_json)), 0)::UBIGINT
             FROM metadata_contract_token_rows token_rows
             JOIN metadata_rows
               ON metadata_rows.source_file = token_rows.metadata_source_file
              AND metadata_rows.source_row_number = token_rows.metadata_source_row_number",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(token_json_bytes > 0);
    assert_eq!(
        estimate.token_relation_peak_bytes,
        token_rows * 64 + representative_rows * 8 + token_json_bytes + 64 * 1024 * 1024
    );
    assert_eq!(estimate.partial_peak_bytes, 64 * 1024 * 1024);
    let representative_json_bytes: u64 = conn
        .query_row(
            "SELECT coalesce(sum(length(rows.metadata_json)), 0)::UBIGINT
             FROM analysis_contracts contracts
             JOIN metadata_rows rows
               ON rows.source_file = contracts.metadata_source_file
              AND rows.source_row_number = contracts.metadata_source_row_number
             WHERE contracts.metadata_contract_index IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        estimate.final_bytes
            >= representative_json_bytes * 16
                + representative_rows * 2_048
                + token_rows * 32
                + 64 * 1024 * 1024
    );
    assert!(estimate.final_bytes >= estimate.partial_peak_bytes.min(estimate.final_bytes));
    assert_eq!(estimate.provisional_feature_bytes, estimate.final_bytes);
    assert!(
        estimate.resident_peak_bytes
            >= representative_json_bytes * 4
                + representative_rows * 2_048
                + token_rows * 24
                + token_json_bytes
                + 64 * 1024 * 1024,
        "resident admission must bound compact feature, atom and routing structures"
    );
    assert!(
        estimate.resident_peak_bytes >= estimate.final_bytes,
        "resident admission must include a global unique-payload/interner envelope"
    );
    assert!(
        estimate.resident_peak_bytes >= estimate.token_relation_peak_bytes,
        "resident admission must cover the complete in-memory token relation"
    );
}

#[test]
fn encode_connection_applies_duckdb_resource_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let mut options = tiny_options(&work, &parquet);
    options.threads = 3;
    options.duckdb_memory_limit = "256MB".into();
    options.temp_directory = Some(work.join("duckdb-temp"));

    let conn = open_prepare_for_encode(&options).unwrap();

    let threads: u64 = conn
        .query_row("SELECT current_setting('threads')::UBIGINT", [], |row| {
            row.get(0)
        })
        .unwrap();
    let memory_limit: String = conn
        .query_row("SELECT current_setting('memory_limit')", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(threads, 3);
    assert!(memory_limit.contains("244.1 MiB"), "actual: {memory_limit}");
    assert!(work.join("duckdb-temp").is_dir());
}

#[test]
fn encode_rejects_underestimated_token_relation_before_loading_it() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let options = tiny_options(&work, &parquet);
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
    let conn = Connection::open(&options.database_path).unwrap();
    let mut estimate = estimate_encode_storage_bytes(&conn).unwrap();
    estimate.token_relation_peak_bytes -= 1;
    let mut broker = StorageBroker::open(&work).unwrap();
    let memory_broker = MemoryBroker::new(16 * GIB, 12 * GIB).unwrap();

    let error = match stream_encode_inputs_with_progress(
        &conn,
        &work,
        &mut broker,
        &memory_broker,
        1,
        estimate,
        |_| {},
    ) {
        Ok(_) => panic!("underestimated token relation was accepted"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("token-source relation admission"),
        "unexpected error: {error}"
    );
}

#[test]
fn encode_stream_reports_frozen_row_totals_and_terminal_completion() {
    use metadata_engine::progress::ProgressPhase;

    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let options = tiny_options(&work, &parquet);
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
    let conn = Connection::open(&options.database_path).unwrap();
    let estimate = estimate_encode_storage_bytes(&conn).unwrap();
    let mut events = Vec::new();
    let mut broker = StorageBroker::open(&work).unwrap();
    let memory_broker = MemoryBroker::new(16 * GIB, 12 * GIB).unwrap();

    stream_encode_inputs_with_progress(
        &conn,
        &work,
        &mut broker,
        &memory_broker,
        2,
        estimate,
        |event| events.push(event),
    )
    .unwrap();

    let collect = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeCollectTokenSources)
        .collect::<Vec<_>>();
    assert!(!collect.is_empty(), "missing source-catalog progress");
    assert_eq!(collect[0].total, None);

    let resolve = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeResolveTokenMemberships)
        .collect::<Vec<_>>();
    assert!(!resolve.is_empty(), "missing token-resolution progress");
    assert_eq!(resolve[0].total, None);
    assert_eq!(resolve.last().unwrap().total, Some(estimate.token_rows));
    assert_eq!(resolve.last().unwrap().completed, estimate.token_rows);

    let source_terminal = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeTokenSources)
        .unwrap();
    assert!(source_terminal.total.unwrap() > 0);
    assert_eq!(source_terminal.completed, source_terminal.total.unwrap());

    let phase_events = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeRows)
        .collect::<Vec<_>>();
    assert!(!phase_events.is_empty(), "missing EncodeRows");
    let terminal = phase_events.last().unwrap();
    assert_eq!(terminal.total, Some(estimate.representative_rows));
    assert_eq!(terminal.completed, estimate.representative_rows);
    let finalize = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeFinalize)
        .unwrap();
    assert_eq!(finalize.completed, finalize.total.unwrap());
}

#[test]
fn optimized_metadata_artifacts_use_revision_two() {
    assert_eq!(ENCODE_SCHEMA_REVISION, 2);
    assert_eq!(BLOCKING_REVISION, 2);
}

fn table_fingerprint(conn: &Connection, table: &str) -> (u64, String) {
    match table {
        "name_atoms" => {
            let count: u64 = conn
                .query_row("SELECT count(*)::UBIGINT FROM name_atoms", [], |row| {
                    row.get(0)
                })
                .unwrap();
            let digest: String = conn
                .query_row(
                    "SELECT coalesce(
                         string_agg(name_norm || ':' || cast(contract_count AS VARCHAR) || ':' || cast(nft_count AS VARCHAR), '|' ORDER BY chain, name_norm),
                         ''
                     )
                     FROM name_atoms",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            (count, digest)
        }
        "metadata_rows" => {
            let count: u64 = conn
                .query_row("SELECT count(*)::UBIGINT FROM metadata_rows", [], |row| {
                    row.get(0)
                })
                .unwrap();
            let digest: String = conn
                .query_row(
                    "SELECT coalesce(
                         string_agg(cast(length(metadata_json) AS VARCHAR) || ':' || md5(metadata_json), '|' ORDER BY source_file, source_row_number),
                         ''
                     )
                     FROM metadata_rows",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            (count, digest)
        }
        "analysis_contracts" => {
            let count: u64 = conn
                .query_row(
                    "SELECT count(*)::UBIGINT FROM analysis_contracts",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let digest: String = conn
                .query_row(
                    "SELECT coalesce(
                         string_agg(chain || ':' || contract_address || ':' || cast(nft_count AS VARCHAR), '|' ORDER BY contract_id),
                         ''
                     )
                     FROM analysis_contracts",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            (count, digest)
        }
        _ => panic!("unsupported fingerprint table {table}"),
    }
}

#[test]
fn match_uses_encode_artifacts_and_releases_payload_dependency() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("tiny.parquet");
    write_tiny_metadata_parquet(&parquet);

    let work = temp.path().join("work");
    let options = tiny_options(&work, &parquet);

    for phase in [
        AnalysisPhase::Prepare,
        AnalysisPhase::MetadataEncode,
        AnalysisPhase::Name,
    ] {
        run_analysis_phase(&options, phase, &work).unwrap();
    }
    fs::remove_file(&options.database_path).unwrap();
    let encode_ready_before_match: serde_json::Value = serde_json::from_slice(
        &fs::read(work.join("checkpoints/metadata-encode.ready.json")).unwrap(),
    )
    .unwrap();
    assert!(encode_ready_before_match["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|artifact| !artifact["path"].as_str().unwrap().contains("payload_blobs")));

    run_analysis_phase(&options, AnalysisPhase::MetadataMatch, &work).unwrap();
    assert!(
        !options.database_path.exists(),
        "MetadataMatch must not reopen or recreate the stage database"
    );

    let encode_dir = work.join(format!(
        "artifacts/metadata/encode-{ENCODE_SCHEMA_REVISION}"
    ));
    let blocking_dir = work.join(format!("artifacts/metadata/blocking-{BLOCKING_REVISION}"));
    assert!(
        encode_dir.join("features.ready").is_file(),
        "missing features.ready under {}",
        encode_dir.display()
    );
    assert!(
        encode_dir.join("source_to_payload.u32").is_file(),
        "missing encode feature arrays"
    );
    assert!(
        blocking_dir.join("blocking.ready").is_file(),
        "missing blocking.ready under {}",
        blocking_dir.display()
    );
    let blocking_ready: serde_json::Value =
        serde_json::from_slice(&fs::read(blocking_dir.join("blocking.ready")).unwrap()).unwrap();
    assert_eq!(
        blocking_ready["atom_count"], 2,
        "scoring-equivalent identities collapse within each chain, not across scopes"
    );
    assert!(work
        .join("checkpoints/metadata-encode.ready.json")
        .is_file());
    let phase_ready: serde_json::Value = serde_json::from_slice(
        &fs::read(work.join("checkpoints/metadata-encode.ready.json")).unwrap(),
    )
    .unwrap();
    assert!(phase_ready["artifacts"].as_array().unwrap().len() > 10);

    let ledger: serde_json::Value =
        serde_json::from_slice(&fs::read(work.join("storage-ledger.json")).unwrap()).unwrap();
    let records = ledger["artifacts"].as_object().unwrap();
    assert!(records
        .values()
        .all(|record| record["class"] != "payload_cas"));
    assert!(!encode_dir.join("payload_blobs").exists());
    assert!(records.values().any(|record| {
        record["class"] == "blocking"
            && record["pins"]
                .as_array()
                .unwrap()
                .iter()
                .any(|pin| pin == "metadata_encode_complete")
    }));

    let summary = work.join("partial/metadata-summary.json");
    assert!(
        summary.is_file(),
        "Match must still produce metadata-summary"
    );
    let encode_partial = work.join("partial/metadata-encode-summary.json");
    let encode_report: serde_json::Value =
        serde_json::from_slice(&fs::read(&encode_partial).unwrap()).unwrap();
    assert_eq!(
        encode_report["summary_rows"].as_array().unwrap().len(),
        0,
        "Encode must not write production summary rows"
    );

    // Re-run Match alone on a fresh partial copy path: summary stays Match-owned.
    let summary_hash = file_sha256(&summary);
    assert_ne!(summary_hash, [0u8; 32]);
    assert!(work
        .join("artifacts/metadata/match-1/metadata-summary-1/metadata-summary.ready")
        .is_file());

    for artifact in phase_ready["artifacts"].as_array().unwrap() {
        assert!(
            std::path::Path::new(artifact["path"].as_str().unwrap()).is_file(),
            "Encode checkpoint must remain valid after CAS collection"
        );
    }
    run_analysis_phase(&options, AnalysisPhase::MetadataMatch, &work).unwrap();
}

#[test]
fn metadata_match_fails_closed_when_artifacts_are_corrupt() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("tiny.parquet");
    write_tiny_metadata_parquet(&parquet);
    let work = temp.path().join("work");
    let options = tiny_options(&work, &parquet);

    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
    run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();
    fs::write(
        work.join("artifacts/metadata/encode-2/fallback_atoms_offsets.u64"),
        b"corrupt",
    )
    .unwrap();

    let error = run_analysis_phase(&options, AnalysisPhase::MetadataMatch, &work).unwrap_err();
    assert!(error.to_string().contains("metadata pipeline"));
    assert!(!work.join("partial/metadata-summary.json").is_file());
}

#[test]
fn encode_preserves_token_specific_metadata_sources() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("sources.parquet");
    let conn = Connection::open_in_memory().unwrap();
    let sql = format!(
        r#"
        CREATE TABLE rows AS SELECT * FROM (VALUES
          ('ethereum','0xaaa','1','','','A','a','{{"description":"representative"}}'),
          ('ethereum','0xaaa','2','','','A','a','{{"description":"token-two"}}'),
          ('ethereum','0xbbb','1','','','B','b','{{ "description" : "representative" }}'),
          ('ethereum','0xbbb','2','','','B','b','{{"description":"token-two"}}')
        ) t(chain,contract_address,token_id,token_uri,image_uri,name,name_norm,metadata_json);
        ALTER TABLE rows ADD COLUMN token_uri_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN image_uri_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN symbol VARCHAR;
        ALTER TABLE rows ADD COLUMN symbol_norm VARCHAR;
        ALTER TABLE rows ADD COLUMN metadata_doc VARCHAR;
        UPDATE rows SET token_uri_norm=token_uri,image_uri_norm=image_uri,
          symbol='',symbol_norm='',metadata_doc=metadata_json;
        COPY rows TO '{path}' (FORMAT PARQUET);
        "#,
        path = parquet.display().to_string().replace('\\', "/")
    );
    conn.execute_batch(&sql).unwrap();

    let work = temp.path().join("work");
    let options = tiny_options(&work, &parquet);
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
    run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();
    let encode_dir = work.join(format!(
        "artifacts/metadata/encode-{ENCODE_SCHEMA_REVISION}"
    ));
    let bundle = EncodeBundle::open(&encode_dir).unwrap();
    let features = bundle.feature_view();
    assert_eq!(&*features.contract_source, &[0, 2]);
    assert_eq!(&*features.token_member_offsets, &[0, 2, 4]);
    assert_eq!(&*features.token_member_sources, &[0, 2, 1, 3]);
    assert_ne!(
        features.source_to_payload[0], features.source_to_payload[1],
        "token-two membership must score its own metadata payload"
    );
    assert_eq!(&*features.fallback_atom_offsets, &[0, 2]);
    assert_eq!(&*features.fallback_atom_contracts, &[0, 1]);
}

#[test]
fn successful_metadata_pipeline_reclaims_payload_cas() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("tiny.parquet");
    write_tiny_metadata_parquet(&parquet);
    let work = temp.path().join("work");
    let options = tiny_options(&work, &parquet);
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
    run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();
    let features = work.join(format!(
        "artifacts/metadata/encode-{ENCODE_SCHEMA_REVISION}"
    ));
    complete_metadata_payload_independence(&features, &work).unwrap();

    let ledger: serde_json::Value =
        serde_json::from_slice(&fs::read(work.join("storage-ledger.json")).unwrap()).unwrap();
    let cas = ledger["artifacts"]
        .as_object()
        .unwrap()
        .values()
        .find(|artifact| artifact["class"] == "payload_cas")
        .cloned();
    assert_eq!(cas, None);
    assert!(!features.join("payload_blobs").exists());
    assert!(ledger["match_independent"].as_array().unwrap().is_empty());
}

#[test]
fn encode_does_not_mutate_prepare_or_name_tables() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("tiny.parquet");
    write_tiny_metadata_parquet(&parquet);

    let work = temp.path().join("work");
    let options = tiny_options(&work, &parquet);
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();

    let before = {
        let conn = Connection::open(&options.database_path).unwrap();
        (
            table_fingerprint(&conn, "name_atoms"),
            table_fingerprint(&conn, "metadata_rows"),
            table_fingerprint(&conn, "analysis_contracts"),
        )
    };

    run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();

    let after = {
        let conn = Connection::open(&options.database_path).unwrap();
        (
            table_fingerprint(&conn, "name_atoms"),
            table_fingerprint(&conn, "metadata_rows"),
            table_fingerprint(&conn, "analysis_contracts"),
        )
    };

    assert_eq!(before.0, after.0, "name_atoms must be unchanged by Encode");
    assert_eq!(
        before.1, after.1,
        "metadata_rows must be unchanged by Encode"
    );
    assert_eq!(
        before.2, after.2,
        "analysis_contracts must be unchanged by Encode"
    );

    assert!(work
        .join(format!(
            "artifacts/metadata/encode-{ENCODE_SCHEMA_REVISION}/features.ready"
        ))
        .is_file());
    assert!(work
        .join(format!(
            "artifacts/metadata/blocking-{BLOCKING_REVISION}/blocking.ready"
        ))
        .is_file());
}
