use super::*;

impl DuckDbFeatureStore {
    pub(super) fn apply_resource_options(
        conn: &Connection,
        options: &DuckDbResourceOptions,
        temp_directory: &Path,
    ) -> Result<(), AppError> {
        let memory_limit = options.memory_limit.replace('\'', "''");
        let temp_directory = temp_directory.to_string_lossy().replace('\'', "''");
        conn.execute_batch(&format!(
            "
            PRAGMA threads={};
            PRAGMA memory_limit='{}';
            PRAGMA temp_directory='{}';
            PRAGMA preserve_insertion_order=false;
            PRAGMA disable_checkpoint_on_shutdown;
            SET checkpoint_threshold='{}';
            SET auto_checkpoint_skip_wal_threshold={};
            SET write_buffer_row_group_count=1;
            ",
            options.threads.max(1),
            memory_limit,
            temp_directory,
            BULK_IMPORT_CHECKPOINT_THRESHOLD,
            BULK_IMPORT_SKIP_WAL_THRESHOLD_BYTES
        ))?;
        Ok(())
    }

    pub(super) fn default_temp_directory(database_path: &str) -> PathBuf {
        if database_path == ":memory:" {
            std::env::temp_dir().join("top_contract_analysis_rs_duckdb")
        } else {
            Path::new(database_path).with_extension("duckdb.tmp")
        }
    }

    pub(super) fn validate_schema(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, "nft_features")?;
        let missing: Vec<&str> = REQUIRED_SNAPSHOT_COLUMNS
            .iter()
            .copied()
            .filter(|column| !columns.contains(*column))
            .collect();
        if !missing.is_empty() {
            return Err(AppError::InvalidData(format!(
                "feature DB nft_features table is missing current required snapshot columns {missing:?}. Rebuild it from a current export-snapshot Parquet file."
            )));
        }

        Ok(())
    }

    pub(super) fn create_contract_representative_table(conn: &Connection) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            CREATE TABLE IF NOT EXISTS {CONTRACT_REPRESENTATIVE_TABLE} (
                chain VARCHAR NOT NULL,
                contract_address VARCHAR NOT NULL,
                name_feature_rowid BIGINT,
                metadata_feature_rowid BIGINT
            );
            "
        ))?;
        Ok(())
    }

    pub(super) fn create_prepared_recall_tables(conn: &Connection) -> Result<(), AppError> {
        Self::create_contract_representative_table(conn)?;
        conn.execute_batch(&format!(
            "
            CREATE TABLE IF NOT EXISTS {FEATURE_GENERATION_TABLE} (
                chain VARCHAR PRIMARY KEY,
                generation_id VARCHAR NOT NULL,
                feature_row_count BIGINT NOT NULL,
                contract_count BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS {NAME_RECALL_ROW_TABLE} (
                chain VARCHAR NOT NULL,
                feature_rowid BIGINT NOT NULL,
                contract_address VARCHAR NOT NULL,
                name_norm VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS {URI_RECALL_POSTING_TABLE} (
                chain VARCHAR NOT NULL,
                uri_kind UTINYINT NOT NULL,
                uri_hash UBIGINT NOT NULL,
                feature_rowid BIGINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS nft_uri_recall_posting_idx
                ON {URI_RECALL_POSTING_TABLE}(chain, uri_kind, uri_hash);
            CREATE INDEX IF NOT EXISTS nft_contract_metadata_cursor_idx
                ON {CONTRACT_REPRESENTATIVE_TABLE}(chain, metadata_feature_rowid);
            DROP TABLE IF EXISTS nft_feature_recall_rows;
            CREATE TABLE IF NOT EXISTS {METADATA_RECALL_DOC_TABLE} (
                chain VARCHAR NOT NULL,
                feature_rowid BIGINT NOT NULL,
                contract_address VARCHAR NOT NULL,
                recall_doc VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS {PREPARED_RECALL_CHAIN_TABLE} (
                chain VARCHAR NOT NULL,
                feature_generation_id VARCHAR,
                feature_row_count BIGINT,
                max_feature_rowid BIGINT,
                feature_fingerprint VARCHAR
            );
            CREATE TABLE IF NOT EXISTS {PREPARE_JOURNAL_TABLE} (
                journal_id UTINYINT PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                input_fingerprint VARCHAR NOT NULL,
                canonical_inputs_json VARCHAR NOT NULL,
                input_manifest_json VARCHAR NOT NULL,
                expected_chains_json VARCHAR NOT NULL,
                imported_generations_json VARCHAR NOT NULL,
                prepared_chains_json VARCHAR NOT NULL,
                phase VARCHAR NOT NULL,
                prepared_format_version VARCHAR NOT NULL,
                normalization_version VARCHAR NOT NULL,
                recall_algorithm_version VARCHAR NOT NULL,
                report_schema_version VARCHAR NOT NULL,
                build_fingerprint VARCHAR NOT NULL,
                started_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                last_error VARCHAR NOT NULL,
                CHECK (journal_id = 1)
            );
            "
        ))?;
        Self::ensure_prepare_journal_columns(conn)?;
        Self::ensure_compact_metadata_recall_doc_schema(conn)?;
        Self::ensure_prepared_recall_chain_columns(conn)?;
        Ok(())
    }

    pub(super) fn ensure_prepare_journal_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, PREPARE_JOURNAL_TABLE)?;
        for column in [
            "input_manifest_json",
            "prepared_format_version",
            "normalization_version",
            "recall_algorithm_version",
            "report_schema_version",
            "build_fingerprint",
        ] {
            if !columns.contains(column) {
                conn.execute(
                    &format!(
                        "ALTER TABLE {PREPARE_JOURNAL_TABLE} ADD COLUMN {column} VARCHAR DEFAULT ''"
                    ),
                    [],
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn ensure_compact_metadata_recall_doc_schema(
        conn: &Connection,
    ) -> Result<(), AppError> {
        let expected = HashSet::from([
            "chain".to_string(),
            "feature_rowid".to_string(),
            "contract_address".to_string(),
            "recall_doc".to_string(),
        ]);
        if Self::table_columns(conn, METADATA_RECALL_DOC_TABLE)? == expected {
            return Ok(());
        }
        // This is a derived table. Recreate old payload-heavy versions and
        // invalidate every prepared generation so no partially migrated DB can
        // be treated as analysis-ready.
        conn.execute_batch(&format!(
            "DROP TABLE {METADATA_RECALL_DOC_TABLE};
             CREATE TABLE {METADATA_RECALL_DOC_TABLE} (
                 chain VARCHAR NOT NULL,
                 feature_rowid BIGINT NOT NULL,
                 contract_address VARCHAR NOT NULL,
                 recall_doc VARCHAR NOT NULL
             );
             DELETE FROM {PREPARED_RECALL_CHAIN_TABLE};"
        ))?;
        Ok(())
    }

    pub(super) fn ensure_prepared_recall_chain_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        for (column, definition) in [
            ("feature_generation_id", "VARCHAR DEFAULT ''"),
            ("feature_row_count", "BIGINT DEFAULT -1"),
            ("max_feature_rowid", "BIGINT DEFAULT -1"),
            ("feature_fingerprint", "VARCHAR DEFAULT ''"),
            ("prepared_format_version", "VARCHAR DEFAULT ''"),
            ("normalization_version", "VARCHAR DEFAULT ''"),
            ("recall_algorithm_version", "VARCHAR DEFAULT ''"),
            ("report_schema_version", "VARCHAR DEFAULT ''"),
            ("build_fingerprint", "VARCHAR DEFAULT ''"),
        ] {
            if !columns.contains(column) {
                conn.execute(
                    &format!(
                        "ALTER TABLE {PREPARED_RECALL_CHAIN_TABLE} ADD COLUMN {column} {definition}"
                    ),
                    [],
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, AppError> {
        let exists = conn.query_row(
            "
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = ?
            )
            ",
            params![table_name],
            |row| row.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    pub(super) fn prepared_recall_state(
        conn: &Connection,
        chain: &str,
    ) -> Result<PreparedRecallState, AppError> {
        let tables_exist = Self::table_exists(conn, CONTRACT_REPRESENTATIVE_TABLE)?
            && Self::table_exists(conn, URI_RECALL_POSTING_TABLE)?
            && Self::table_exists(conn, METADATA_RECALL_DOC_TABLE)?
            && Self::table_exists(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        if !tables_exist {
            return Ok(PreparedRecallState { ready: false });
        }
        let prepared_columns = Self::table_columns(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        if [
            "feature_generation_id",
            "prepared_format_version",
            "normalization_version",
            "recall_algorithm_version",
            "report_schema_version",
            "build_fingerprint",
        ]
        .iter()
        .any(|column| !prepared_columns.contains(*column))
        {
            return Ok(PreparedRecallState { ready: false });
        }
        let current_generation = Self::feature_generation_state(conn, chain)?;
        let prepared_identity = {
            let mut stmt = conn.prepare(&format!(
                "SELECT coalesce(feature_generation_id, ''),
                        coalesce(prepared_format_version, ''),
                        coalesce(normalization_version, ''),
                        coalesce(recall_algorithm_version, ''),
                        coalesce(report_schema_version, ''),
                        coalesce(build_fingerprint, '')
                 FROM {PREPARED_RECALL_CHAIN_TABLE}
                 WHERE chain = ? LIMIT 1"
            ))?;
            let mut rows = stmt.query_map(params![chain], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?;
            match rows.next() {
                Some(value) => Some(value?),
                None => None,
            }
        };
        Ok(PreparedRecallState {
            ready: current_generation.as_ref().is_some_and(|state| {
                prepared_identity.as_ref().is_some_and(
                    |(generation, prepared, normalization, algorithm, report, build)| {
                        state.generation_id == *generation
                            && prepared == PREPARED_FORMAT_VERSION
                            && normalization == NORMALIZATION_VERSION
                            && algorithm == RECALL_ALGORITHM_VERSION
                            && report == REPORT_SCHEMA_VERSION
                            && build == BUILD_FINGERPRINT
                    },
                )
            }),
        })
    }
}
