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
            CREATE TABLE IF NOT EXISTS {FEATURE_RECALL_TABLE} (
                chain VARCHAR NOT NULL,
                feature_rowid BIGINT NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                token_uri_norm VARCHAR,
                image_uri_norm VARCHAR,
                name_norm VARCHAR,
                name_sort VARCHAR
            );
            CREATE TABLE IF NOT EXISTS {METADATA_RECALL_DOC_TABLE} (
                chain VARCHAR NOT NULL,
                feature_rowid BIGINT NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                token_uri_norm VARCHAR,
                image_uri_norm VARCHAR,
                name_norm VARCHAR,
                recall_doc VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS {PREPARED_RECALL_CHAIN_TABLE} (
                chain VARCHAR NOT NULL,
                feature_row_count BIGINT,
                max_feature_rowid BIGINT,
                feature_fingerprint VARCHAR
            );
            "
        ))?;
        Self::ensure_prepared_recall_chain_columns(conn)?;
        Ok(())
    }

    pub(super) fn ensure_prepared_recall_chain_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        for (column, definition) in [
            ("feature_row_count", "BIGINT DEFAULT -1"),
            ("max_feature_rowid", "BIGINT DEFAULT -1"),
            ("feature_fingerprint", "VARCHAR DEFAULT ''"),
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
            && Self::table_exists(conn, FEATURE_RECALL_TABLE)?
            && Self::table_exists(conn, METADATA_RECALL_DOC_TABLE)?
            && Self::table_exists(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        if !tables_exist {
            return Ok(PreparedRecallState { ready: false });
        }
        let prepared_columns = Self::table_columns(conn, PREPARED_RECALL_CHAIN_TABLE)?;
        if !prepared_columns.contains("feature_row_count")
            || !prepared_columns.contains("max_feature_rowid")
            || !prepared_columns.contains("feature_fingerprint")
        {
            return Ok(PreparedRecallState { ready: false });
        }
        let current_stats = Self::feature_chain_stats(conn, chain)?;
        let prepared_stats = Self::prepared_recall_chain_stats(conn, chain)?;
        let recall_row_count = Self::chain_row_count(conn, FEATURE_RECALL_TABLE, chain)?;
        Ok(PreparedRecallState {
            ready: prepared_stats.as_ref() == Some(&current_stats)
                && recall_row_count == current_stats.row_count,
        })
    }

    pub(super) fn chain_row_count(
        conn: &Connection,
        table_name: &str,
        chain: &str,
    ) -> Result<i64, AppError> {
        if !Self::table_exists(conn, table_name)? {
            return Ok(0);
        }
        let count = conn.query_row(
            &format!("SELECT CAST(count(*) AS BIGINT) FROM {table_name} WHERE chain = ?"),
            params![chain],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count)
    }
}
