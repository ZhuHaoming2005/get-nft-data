use super::*;

impl DuckDbFeatureStore {
    pub(super) fn drop_obsolete_nft_feature_columns(conn: &Connection) -> Result<(), AppError> {
        let columns = Self::table_columns(conn, "nft_features")?;
        for column in ["metadata_doc", "name_prefix8", "metadata_keywords_arr"] {
            if columns.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE nft_features DROP COLUMN {column}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn table_columns(conn: &Connection, table_name: &str) -> Result<HashSet<String>, AppError> {
        let mut stmt = conn.prepare(&format!("DESCRIBE {table_name}"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut columns = HashSet::new();
        for row in rows {
            columns.insert(row?);
        }
        Ok(columns)
    }

    pub fn has_chain_rows(&self, chain: &str) -> Result<bool, AppError> {
        let conn = self.conn()?;
        let exists = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM nft_features WHERE chain = ? LIMIT 1)",
            params![chain],
            |row| row.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    pub fn replace_chain_rows(
        &self,
        chain: &str,
        rows: &[DatabaseNftRecord],
    ) -> Result<(), AppError> {
        self.invalidate_metadata_recall_index(chain)?;
        let mut conn = self.conn()?;
        let transaction = conn.transaction()?;
        transaction.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;

        // Appender is materially faster than a per-row prepared INSERT for bulk
        // loads. The base table is committed here and the derived recall tables
        // are rebuilt below, so appending within this transaction is safe.
        // `nft_features` always has exactly these 11 columns (CREATE TABLE +
        // `drop_obsolete_nft_feature_columns` + `validate_schema` enforce it),
        // so a full-table appender matches the previous explicit column list.
        let mut appender = transaction.appender("nft_features")?;
        for row in rows {
            let contract_address = row.contract_address.to_lowercase();
            let token_uri_norm = normalize_url(&row.token_uri).unwrap_or_default();
            let image_uri_norm = normalize_url(&row.image_uri).unwrap_or_default();
            let name_norm = normalize_name(&row.name);
            appender.append_row(params![
                chain,
                contract_address,
                row.token_id,
                row.token_uri,
                row.image_uri,
                row.name,
                row.symbol,
                row.metadata_json,
                token_uri_norm,
                image_uri_norm,
                name_norm,
            ])?;
        }
        appender.flush()?;
        drop(appender);
        transaction.execute(
            &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        transaction.commit()?;
        Self::refresh_prepared_recall_tables_for_chain(&conn, chain)?;
        Ok(())
    }

    pub(super) fn load_parquet_dataset_via_duckdb(
        &self,
        chain: &str,
        parquet_path: &str,
    ) -> Result<(), AppError> {
        self.invalidate_metadata_recall_index(chain)?;
        let path = Self::sql_string_literal(&parquet_path.replace('\\', "/"));
        let insert_sql = format!(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm
            )
            SELECT
                ? AS chain,
                lower(CAST(contract_address AS VARCHAR)) AS contract_address,
                CAST(token_id AS VARCHAR) AS token_id,
                coalesce(CAST(token_uri AS VARCHAR), '') AS token_uri,
                coalesce(CAST(image_uri AS VARCHAR), '') AS image_uri,
                coalesce(CAST(name AS VARCHAR), '') AS name,
                coalesce(CAST(symbol AS VARCHAR), '') AS symbol,
                coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm
            FROM read_parquet({path})
            ",
        );
        let mut conn = self.conn()?;
        let transaction = conn.transaction()?;
        transaction.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;
        transaction.execute(&insert_sql, params![chain])?;
        transaction.execute(
            &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        transaction.commit()?;
        Self::refresh_prepared_recall_tables_for_chain(&conn, chain)?;
        Ok(())
    }

    pub fn load_parquet_dataset(&self, chain: &str, parquet_path: &str) -> Result<(), AppError> {
        let parquet_path_literal = Self::sql_string_literal(&parquet_path.replace('\\', "/"));
        let probe_sql = format!("DESCRIBE SELECT * FROM read_parquet({parquet_path_literal})");
        let column_names: HashSet<String> = {
            let conn = self.conn()?;
            let mut stmt = conn.prepare(&probe_sql)?;
            let describe_rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut column_names = HashSet::new();
            for row in describe_rows {
                column_names.insert(row?);
            }
            column_names
        };

        let missing: Vec<&str> = REQUIRED_SNAPSHOT_COLUMNS
            .iter()
            .copied()
            .filter(|column| !column_names.contains(*column))
            .collect();
        if !missing.is_empty() {
            return Err(AppError::InvalidData(format!(
                "Parquet file {parquet_path:?} is missing required snapshot columns {missing:?}. Re-export the snapshot with the current export-snapshot command."
            )));
        }

        self.load_parquet_dataset_via_duckdb(chain, parquet_path)
    }

    pub fn load_parquet_dataset_if_chain_missing(
        &self,
        chain: &str,
        parquet_path: &str,
    ) -> Result<bool, AppError> {
        if self.has_chain_rows(chain)? {
            return Ok(false);
        }
        self.load_parquet_dataset(chain, parquet_path)?;
        Ok(true)
    }

    pub(super) fn sql_string_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    pub(super) fn sql_metadata_json_eligible_predicate(column: &str) -> String {
        // Keep in sync with name_uri_analysis_rs::metadata_json_eligible_predicate
        // and metadata_is_dedup_eligible: trim, non-empty, len<=64KiB, starts with { or [
        let trimmed = format!("trim(coalesce(CAST({column} AS VARCHAR), ''))");
        format!(
            "length({trimmed}) <= {MAX_METADATA_BYTES_FOR_DEDUP} \
             AND ({trimmed} LIKE '{{%' \
             OR {trimmed} LIKE '[%')"
        )
    }

}
