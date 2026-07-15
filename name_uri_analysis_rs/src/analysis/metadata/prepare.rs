use duckdb::Connection;

use super::super::{execute_progress_batch, AnalysisError, ProgressTracker};

pub(crate) const MAX_METADATA_BYTES_FOR_DEDUP: usize =
    metadata_engine::encode::MAX_METADATA_BYTES_FOR_DEDUP;

pub(super) fn metadata_is_dedup_eligible(raw: &str) -> bool {
    let raw = raw.trim();
    !raw.is_empty()
        && raw.len() <= MAX_METADATA_BYTES_FOR_DEDUP
        && matches!(raw.chars().next(), Some('{') | Some('['))
}

pub(crate) fn prepare_metadata_compact_tables(
    conn: &Connection,
    progress: &ProgressTracker,
) -> Result<(), AnalysisError> {
    progress.start_stage("preparing compact metadata sources", 1);
    execute_progress_batch(
        conn,
        metadata_contract_token_rows_sql(),
        progress,
        "filtered singleton token IDs and materialized compact sources",
    )?;
    progress.finish_stage("compact metadata sources ready");
    Ok(())
}

pub(super) fn metadata_contract_token_rows_sql() -> &'static str {
    "
        DROP TABLE IF EXISTS metadata_contract_token_rows;
        DROP TABLE IF EXISTS metadata_token_stats;
        DROP TABLE IF EXISTS metadata_token_dictionary;
        CREATE TEMP TABLE metadata_unique_contract_tokens AS
        SELECT c.metadata_contract_index AS contract_index,
               a.token_id,
               arg_min(
                   struct_pack(
                       file_id := a.source_file,
                       row_number := a.source_row_number
                   ),
                   row(a.source_file, a.source_row_number)
               ) AS metadata_source,
               max(octet_length(encode(a.metadata_json)))::UBIGINT
                   AS metadata_max_json_bytes
        FROM metadata_rows a
        JOIN analysis_contracts c
          ON c.contract_id = a.contract_id
        WHERE a.token_id <> ''
          AND c.metadata_contract_index IS NOT NULL
          AND a.metadata_eligible
        GROUP BY c.metadata_contract_index, a.token_id;

        CREATE TEMP TABLE metadata_token_frequencies AS
        SELECT token_id, count(*)::UBIGINT AS contract_frequency
        FROM metadata_unique_contract_tokens
        GROUP BY token_id;

        CREATE TABLE metadata_token_stats AS
        SELECT count(*) FILTER (WHERE contract_frequency = 1)::UBIGINT
                   AS singleton_token_count,
               count(*) FILTER (WHERE contract_frequency >= 2)::UBIGINT
                   AS retained_shared_token_count
        FROM metadata_token_frequencies;

        CREATE TABLE metadata_token_dictionary AS
        SELECT token_id,
               (row_number() OVER (ORDER BY token_id) - 1)::BIGINT AS token_index
        FROM metadata_token_frequencies
        WHERE contract_frequency >= 2;

        CREATE TABLE metadata_contract_token_rows AS
        SELECT metadata.contract_index,
               retained.token_index,
               metadata.metadata_source.file_id::UINTEGER AS metadata_source_file,
               metadata.metadata_source.row_number::UBIGINT AS metadata_source_row_number,
               metadata.metadata_max_json_bytes
        FROM metadata_unique_contract_tokens metadata
        INNER JOIN metadata_token_dictionary retained USING (token_id);

        DROP TABLE metadata_unique_contract_tokens;
        DROP TABLE metadata_token_frequencies;
    "
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eligibility_matches_prepare_sql_contract() {
        assert_eq!(
            MAX_METADATA_BYTES_FOR_DEDUP,
            metadata_engine::encode::MAX_METADATA_BYTES_FOR_DEDUP
        );
        assert!(metadata_is_dedup_eligible("  {\"a\":1}"));
        assert!(metadata_is_dedup_eligible("\n[1]"));
        assert!(!metadata_is_dedup_eligible("  x{}"));
        assert!(!metadata_is_dedup_eligible(""));
        assert!(!metadata_is_dedup_eligible(&format!(
            "{{\"value\":\"{}\"}}",
            "x".repeat(MAX_METADATA_BYTES_FOR_DEDUP)
        )));
    }

    #[test]
    fn compact_source_sql_uses_stable_source_coordinates() {
        let sql = metadata_contract_token_rows_sql();
        assert!(!sql.contains("rowid"));
        assert!(sql.contains("metadata_source_file"));
        assert!(sql.contains("metadata_source_row_number"));
    }
}
