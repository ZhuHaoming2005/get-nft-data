use std::collections::HashMap;
use std::fs;
use std::path::Path;

use duckdb::Connection;
use sha2::{Digest, Sha256};

use crate::analysis::{run_analysis_phase, AnalysisOptions, AnalysisPhase};
use metadata_engine::blocking::BLOCKING_REVISION;
use metadata_engine::encode::{EncodeBundle, ENCODE_SCHEMA_REVISION};
use metadata_engine::resource::{MemoryBroker, GIB};
use metadata_engine::storage::StorageBroker;

use super::super::encode::{
    estimate_encode_storage_bytes, fallback_contract_candidates_sql, finish_ordered_group_count,
    first_usable_rows_by_ordered_group, observe_ordered_group, open_prepare_for_encode,
    ordered_group_ranges, resolve_fallback_contracts, retained_token_candidates_sql,
    stream_encode_inputs_with_progress,
};

#[test]
fn fallback_resolution_checks_presence_only_until_its_first_usable_row() {
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
        metadata_engine::encode::metadata_has_prefilter_tokens(json)
    });

    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].0, 1);
    assert_eq!(resolved[0].1, r#"{"description":"first"}"#);
    assert_eq!(resolved[1].0, 2);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "resolution must short-circuit per contract without a full parse"
    );
}

#[test]
fn production_group_selector_short_circuits_and_skips_a_selected_cross_batch_group() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let first_keys = [1u32, 1, 1, 2, 2];
    let first_usable = [false, true, true, true, true];
    let first_ranges = ordered_group_ranges(first_keys.len(), |index| first_keys[index]);
    let calls = AtomicUsize::new(0);
    let selected = first_usable_rows_by_ordered_group(&first_ranges, None, |index| {
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(first_usable[index])
    })
    .unwrap();
    assert_eq!(selected, vec![Some(1), Some(3)]);
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    // The first group continues in the next Arrow batch. Once its source was
    // selected in the previous batch, none of its JSON rows may be parsed.
    let next_keys = [2u32, 2, 3, 3];
    let next_ranges = ordered_group_ranges(next_keys.len(), |index| next_keys[index]);
    let cross_batch_calls = AtomicUsize::new(0);
    let selected = first_usable_rows_by_ordered_group(&next_ranges, Some(2), |index| {
        cross_batch_calls.fetch_add(1, Ordering::Relaxed);
        if next_keys[index] == 2 {
            panic!("selected cross-batch group must not inspect JSON");
        }
        Ok(index == 3)
    })
    .unwrap();
    assert_eq!(selected, vec![None, Some(3)]);
    assert_eq!(cross_batch_calls.load(Ordering::Relaxed), 2);
}

#[test]
fn ordered_group_progress_completes_cross_batch_groups_once() {
    let mut current = None;
    let mut completed = 0u64;

    for group in [1u32, 1] {
        observe_ordered_group(group, &mut current, &mut completed);
    }
    assert_eq!(completed, 0, "the open tail group is not complete yet");

    for group in [1u32, 2] {
        observe_ordered_group(group, &mut current, &mut completed);
    }
    assert_eq!(
        completed, 1,
        "the repeated batch-head group is not recounted"
    );

    for group in [2u32, 3] {
        observe_ordered_group(group, &mut current, &mut completed);
    }
    assert_eq!(completed, 2);
    assert_eq!(finish_ordered_group_count(current, completed), 3);
}

#[test]
fn retained_token_sources_use_one_streaming_query_without_encode_temp_tables() {
    let sql = retained_token_candidates_sql();

    assert!(sql.contains("metadata_contract_token_rows"));
    assert!(sql.contains("ORDER BY token_rows.contract_index"));
    assert!(!sql.to_ascii_uppercase().contains("CREATE TEMP TABLE"));
}

#[test]
fn fallback_contract_filter_uses_the_bounded_appender_table() {
    let sql = fallback_contract_candidates_sql();

    assert!(sql.contains("encode_fallback_contracts"));
    assert!(!sql.contains("unnest(?::UINTEGER[])"));
    assert!(!sql.contains("arrow("));
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

fn write_fallback_metadata_parquet(path: &Path) {
    let _ = fs::remove_file(path);
    let conn = Connection::open_in_memory().unwrap();
    let sql = format!(
        r#"
        CREATE TABLE rows AS
        SELECT * FROM (VALUES
            ('ethereum', '0xfallback', '1', '', '', 'Fallback', 'fallback', '{{}}'),
            ('ethereum', '0xfallback', '2', '', '', 'Fallback', 'fallback', '{{"description":"usable source"}}')
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

#[allow(dead_code)] // retained for optional byte-level diagnostics outside semantic oracle
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

fn prepare_business_fingerprint(database: &Path) -> String {
    let conn = Connection::open(database).unwrap();
    // Business identity only: dense internal IDs may differ across threads.
    let contracts: String = conn
        .query_row(
            "SELECT string_agg(chain || ':' || contract_address, '|'
                 ORDER BY chain, contract_address)
             FROM analysis_contracts
             WHERE metadata_contract_index IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let tokens: String = conn
        .query_row(
            "SELECT coalesce(string_agg(token_id, '|' ORDER BY token_id), '')
             FROM metadata_token_dictionary",
            [],
            |row| row.get(0),
        )
        .unwrap();
    format!("{contracts}#{tokens}")
}

#[test]
fn prepare_business_identities_are_stable_across_thread_counts() {
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let mut fingerprints = Vec::new();
    for threads in [1, 4] {
        let work = dir.path().join(format!("prepare-{threads}"));
        fs::create_dir_all(&work).unwrap();
        let mut options = tiny_options(&work, &parquet);
        options.threads = threads;
        run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();
        fingerprints.push(prepare_business_fingerprint(&options.database_path));
    }
    assert_eq!(fingerprints[0], fingerprints[1]);
}

#[test]
fn parallel_encode_match_is_semantically_deterministic_across_thread_counts() {
    use crate::analysis::semantic_oracle::summaries_semantically_equal;
    use crate::analysis::types::AnalysisReport;

    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("metadata.parquet");
    write_tiny_metadata_parquet(&parquet);
    let prepared = dir.path().join("prepared");
    fs::create_dir_all(&prepared).unwrap();
    let prepared_options = tiny_options(&prepared, &parquet);
    run_analysis_phase(&prepared_options, AnalysisPhase::Prepare, &prepared).unwrap();

    let mut summaries = Vec::new();
    for threads in [1, 4] {
        let work = dir.path().join(format!("work-{threads}"));
        fs::create_dir_all(&work).unwrap();
        let mut options = tiny_options(&work, &parquet);
        options.threads = threads;
        fs::copy(&prepared_options.database_path, &options.database_path).unwrap();
        run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();
        run_analysis_phase(&options, AnalysisPhase::MetadataMatch, &work).unwrap();
        let summary_path = work.join("partial/metadata-summary.json");
        let report: AnalysisReport =
            serde_json::from_slice(&fs::read(&summary_path).unwrap()).unwrap();
        summaries.push(report.summary_rows);
        assert!(
            !metadata_engine::pipeline::default_output_dir(&work)
                .join("component-snapshots")
                .exists(),
            "Ephemeral Match must not persist component roots"
        );
    }
    assert!(
        summaries_semantically_equal(&summaries[0], &summaries[1]),
        "thread=1 vs thread=4 metadata summaries must match semantically:\n{:#?}\n{:#?}",
        summaries[0],
        summaries[1]
    );
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
            "SELECT coalesce(sum(source_json_bytes), 0)::UBIGINT
             FROM (
                 SELECT max(length(metadata_rows.metadata_json))::UBIGINT AS source_json_bytes
                 FROM metadata_contract_token_rows token_rows
                 JOIN metadata_rows
                   ON metadata_rows.source_file = token_rows.metadata_source_file
                  AND metadata_rows.source_row_number = token_rows.metadata_source_row_number
                 GROUP BY token_rows.metadata_source_file,
                          token_rows.metadata_source_row_number
             ) distinct_sources",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(token_json_bytes > 0);
    let admitted_token_json_bytes = token_json_bytes * 5 / 4;
    assert_eq!(
        estimate.token_relation_peak_bytes,
        token_rows * 192 + representative_rows * 8 + admitted_token_json_bytes + 64 * 1024 * 1024
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
    use crate::analysis::{PipelineStage, ProgressTracker};
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
    assert_phase_progress_monotonic(&collect);

    let resolve = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeResolveTokenMemberships)
        .collect::<Vec<_>>();
    assert!(!resolve.is_empty(), "missing token-resolution progress");
    assert_phase_progress_monotonic(&resolve);

    let sort_memberships = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeSortTokenMemberships)
        .collect::<Vec<_>>();
    assert!(
        !sort_memberships.is_empty(),
        "missing membership-sort progress"
    );
    assert_phase_progress_monotonic(&sort_memberships);
    assert_eq!(
        sort_memberships.last().unwrap().completed,
        estimate.token_rows
    );

    let prepare_fallback = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodePrepareFallbackTokenSources)
        .collect::<Vec<_>>();
    assert!(
        !prepare_fallback.is_empty(),
        "missing in-memory fallback-prepare progress"
    );
    assert_phase_progress_monotonic(&prepare_fallback);
    assert_eq!(prepare_fallback[0].total, Some(0));
    assert_eq!(
        prepare_fallback[0].unit,
        metadata_engine::progress::WorkUnit::Items
    );

    let classify_fallback = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeTokenFallbackSources)
        .collect::<Vec<_>>();
    assert!(
        !classify_fallback.is_empty(),
        "missing fallback classify progress"
    );
    assert_phase_progress_monotonic(&classify_fallback);
    assert_eq!(
        classify_fallback[0].unit,
        metadata_engine::progress::WorkUnit::Items
    );

    let source_terminal = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeTokenSources)
        .unwrap();
    assert_eq!(source_terminal.total, Some(estimate.token_rows));
    assert_eq!(source_terminal.completed, estimate.token_rows);
    assert_eq!(source_terminal.unit.label(), "token groups");
    let rendered = ProgressTracker::for_pipeline_stage(PipelineStage::MetadataEncode, true);
    rendered.observe_engine_event(*source_terminal);
    let ProgressTracker::Enabled { metrics, .. } = &rendered else {
        panic!("progress must be enabled");
    };
    assert!(
        metrics
            .message()
            .contains(&format!("selected {} sources", estimate.token_rows)),
        "{}",
        metrics.message()
    );
    assert!(
        !metrics.message().contains("matched"),
        "{}",
        metrics.message()
    );

    let read_events = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeReadRepresentatives)
        .collect::<Vec<_>>();
    assert!(!read_events.is_empty(), "missing EncodeReadRepresentatives");
    let read_terminal = read_events.last().unwrap();
    assert_eq!(read_terminal.total, Some(estimate.representative_rows));
    assert_eq!(read_terminal.completed, estimate.representative_rows);

    let register_events = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeRegisterPayloads)
        .collect::<Vec<_>>();
    assert!(
        !register_events.is_empty(),
        "missing EncodeRegisterPayloads"
    );
    let register_terminal = register_events.last().unwrap();
    assert_eq!(register_terminal.total, Some(estimate.representative_rows));
    assert_eq!(register_terminal.completed, estimate.representative_rows);

    let parse_unique = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeParseUniquePayloads)
        .unwrap();
    assert_eq!(parse_unique.completed, parse_unique.total.unwrap());

    let term_dictionary = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeBuildTermDictionary)
        .unwrap();
    assert_eq!(term_dictionary.completed, term_dictionary.total.unwrap());

    let build_columns = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeBuildColumns)
        .unwrap();
    assert_eq!(build_columns.completed, build_columns.total.unwrap());

    let build_atoms = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeBuildAtoms)
        .unwrap();
    assert_eq!(build_atoms.completed, build_atoms.total.unwrap());

    let finalize = events
        .iter()
        .rfind(|event| event.phase == ProgressPhase::EncodeFinalize)
        .unwrap();
    assert_eq!(finalize.completed, finalize.total.unwrap());
}

#[test]
fn fallback_source_progress_is_one_exact_contract_task() {
    use metadata_engine::progress::{ProgressPhase, WorkUnit};

    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let parquet = dir.path().join("fallback-metadata.parquet");
    write_fallback_metadata_parquet(&parquet);
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

    assert!(events
        .iter()
        .all(|event| event.phase != ProgressPhase::EncodeResolveFallbacks));
    let fallback = events
        .iter()
        .filter(|event| event.phase == ProgressPhase::EncodeFallbackSources)
        .collect::<Vec<_>>();
    assert!(!fallback.is_empty(), "missing fallback source progress");
    assert_phase_progress_monotonic(&fallback);
    assert!(fallback.iter().all(|event| event.total == Some(1)));
    assert!(fallback
        .iter()
        .all(|event| event.unit == WorkUnit::Contracts));
    let terminal = fallback.last().unwrap();
    assert_eq!(terminal.completed, 1);
    assert_eq!(terminal.counters.candidates, 2);
    assert_eq!(terminal.counters.selected, 1);
}

fn assert_phase_progress_monotonic(events: &[&metadata_engine::progress::ProgressEvent]) {
    let first = events[0];
    for window in events.windows(2) {
        let previous = window[0];
        let next = window[1];
        assert_eq!(previous.total, first.total, "phase total must stay stable");
        assert_eq!(next.total, first.total, "phase total must stay stable");
        assert_eq!(previous.unit, first.unit, "phase unit must stay stable");
        assert_eq!(next.unit, first.unit, "phase unit must stay stable");
        assert!(
            next.completed >= previous.completed,
            "phase completed must be monotonic"
        );
    }
    let last = events.last().unwrap();
    if let Some(total) = last.total {
        assert_eq!(last.completed, total);
    }
}

#[test]
fn optimized_metadata_artifacts_use_revision_three() {
    assert_eq!(ENCODE_SCHEMA_REVISION, 3);
    assert_eq!(BLOCKING_REVISION, 3);
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
    assert!(
        !encode_dir.join("payload_blobs").exists(),
        "full-memory Encode must not create payload_blobs"
    );
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
    assert!(!work
        .join("artifacts/metadata/match-1/metadata-summary-1/metadata-summary.ready")
        .exists());
    assert!(!work
        .join("artifacts/metadata/match-1/component-snapshots")
        .exists());

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
        work.join("artifacts/metadata/encode-3/fallback_atoms_offsets.u64"),
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
    let prepared = Connection::open(&options.database_path).unwrap();
    let mut statement = prepared
        .prepare("SELECT token_index::UINTEGER, token_id FROM metadata_token_dictionary")
        .unwrap();
    let token_ids = statement
        .query_map([], |row| {
            Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<HashMap<_, _>, _>>()
        .unwrap();
    for token_index in 0..features.token_member_offsets.len() - 1 {
        let token_id = token_ids.get(&(token_index as u32)).unwrap();
        let begin = features.token_member_offsets[token_index] as usize;
        let end = features.token_member_offsets[token_index + 1] as usize;
        for member in begin..end {
            let contract = features.token_member_contracts[member] as usize;
            let source = features.token_member_sources[member] as usize;
            let representative = features.contract_source[contract] as usize;
            if token_id == "1" {
                assert_eq!(source, representative);
            } else {
                assert_eq!(token_id, "2");
                assert_ne!(
                    features.source_to_payload[source], features.source_to_payload[representative],
                    "token-two membership must score its own metadata payload"
                );
            }
        }
    }
    assert_eq!(&*features.fallback_atom_offsets, &[0, 2]);
    assert_eq!(&*features.fallback_atom_contracts, &[0, 1]);
}

#[test]
fn successful_metadata_encode_never_creates_payload_cas() {
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
}

#[test]
fn encode_clears_stale_payload_blobs_from_prior_cas_revision() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("tiny.parquet");
    write_tiny_metadata_parquet(&parquet);
    let work = temp.path().join("work");
    let options = tiny_options(&work, &parquet);
    run_analysis_phase(&options, AnalysisPhase::Prepare, &work).unwrap();

    let encode_dir = work.join(format!(
        "artifacts/metadata/encode-{ENCODE_SCHEMA_REVISION}"
    ));
    let stale_blobs = encode_dir.join("payload_blobs");
    fs::create_dir_all(&stale_blobs).unwrap();
    fs::write(stale_blobs.join("pack-0000.bin"), b"stale-cas-pack").unwrap();
    fs::write(stale_blobs.join("payload_digests.bin"), b"stale").unwrap();

    run_analysis_phase(&options, AnalysisPhase::MetadataEncode, &work).unwrap();

    assert!(
        !encode_dir.join("payload_blobs").exists(),
        "Encode must delete leftover payload_blobs before publish"
    );
    let ledger: serde_json::Value =
        serde_json::from_slice(&fs::read(work.join("storage-ledger.json")).unwrap()).unwrap();
    assert!(ledger["artifacts"]
        .as_object()
        .unwrap()
        .values()
        .all(|artifact| {
            artifact["path"]
                .as_str()
                .map(|path| !path.contains("payload_blobs"))
                .unwrap_or(true)
        }));
    let ready: serde_json::Value = serde_json::from_slice(
        &fs::read(work.join("checkpoints/metadata-encode.ready.json")).unwrap(),
    )
    .unwrap();
    assert!(ready["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|artifact| { !artifact["path"].as_str().unwrap().contains("payload_blobs") }));
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
