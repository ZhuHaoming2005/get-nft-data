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

    pub(super) fn table_columns(
        conn: &Connection,
        table_name: &str,
    ) -> Result<HashSet<String>, AppError> {
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

    pub fn chain_totals(&self, chain: &str) -> Result<crate::models::ChainTotalsPayload, AppError> {
        let conn = self.conn()?;
        let (total_nfts, total_contracts) = conn.query_row(
            "SELECT CAST(count(*) AS BIGINT), \
                    CAST(count(DISTINCT contract_address) AS BIGINT) \
             FROM nft_features WHERE chain = ?",
            params![chain],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        Ok(crate::models::ChainTotalsPayload {
            total_nfts,
            total_contracts,
        })
    }

    pub fn snapshot_identity(&self, chain: &str) -> Result<String, AppError> {
        if let Some(identity) = self
            .snapshot_identity_cache
            .lock()
            .map_err(|err| {
                AppError::DuckDb(format!("snapshot identity cache lock poisoned: {err}"))
            })?
            .get(chain)
            .cloned()
        {
            return Ok(identity);
        }
        let conn = self.conn()?;
        let identity = conn.query_row(
            "SELECT concat(
                 CAST(count(*) AS VARCHAR), ':',
                 CAST(count(DISTINCT contract_address) AS VARCHAR), ':',
                 coalesce(CAST(bit_xor(hash(
                     contract_address, token_id, token_uri, image_uri, name, symbol,
                     metadata_json, token_uri_norm, image_uri_norm, name_norm
                 )) AS VARCHAR), '0')
             )
             FROM nft_features WHERE chain = ?",
            params![chain],
            |row| row.get::<_, String>(0),
        )?;
        drop(conn);
        self.snapshot_identity_cache
            .lock()
            .map_err(|err| {
                AppError::DuckDb(format!("snapshot identity cache lock poisoned: {err}"))
            })?
            .insert(chain.to_string(), identity.clone());
        Ok(identity)
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
            let contract_address = if chain.trim().eq_ignore_ascii_case("solana") {
                row.contract_address.trim().to_string()
            } else {
                row.contract_address.trim().to_lowercase()
            };
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
        let contract_address_sql = if chain.trim().eq_ignore_ascii_case("solana") {
            "trim(CAST(contract_address AS VARCHAR))"
        } else {
            "lower(CAST(contract_address AS VARCHAR))"
        };
        let insert_sql = format!(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name, symbol, metadata_json,
                token_uri_norm, image_uri_norm, name_norm
            )
            SELECT
                ? AS chain,
                {contract_address_sql} AS contract_address,
                CAST(token_id AS VARCHAR) AS token_id,
                coalesce(CAST(token_uri AS VARCHAR), '') AS token_uri,
                coalesce(CAST(image_uri AS VARCHAR), '') AS image_uri,
                coalesce(CAST(name AS VARCHAR), '') AS name,
                coalesce(CAST(symbol AS VARCHAR), '') AS symbol,
                coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY {contract_address_sql}, CAST(token_id AS VARCHAR)
                    ORDER BY
                        ((trim(coalesce(CAST(token_uri AS VARCHAR), '')) <> '')::INTEGER
                         + (trim(coalesce(CAST(image_uri AS VARCHAR), '')) <> '')::INTEGER
                         + (trim(coalesce(CAST(name AS VARCHAR), '')) <> '')::INTEGER
                         + (trim(coalesce(CAST(metadata_json AS VARCHAR), '')) <> '')::INTEGER
                         + (trim(coalesce(CAST(token_uri_norm AS VARCHAR), '')) <> '')::INTEGER
                         + (trim(coalesce(CAST(image_uri_norm AS VARCHAR), '')) <> '')::INTEGER
                         + (trim(coalesce(CAST(name_norm AS VARCHAR), '')) <> '')::INTEGER) DESC,
                        length(coalesce(CAST(metadata_json AS VARCHAR), '')) DESC,
                        CAST(token_uri AS VARCHAR), CAST(image_uri AS VARCHAR),
                        CAST(name AS VARCHAR), CAST(metadata_json AS VARCHAR)
                ) AS source_rank
                FROM read_parquet({path})
                WHERE lower(trim(CAST(chain AS VARCHAR))) = ?
            ) source
            WHERE source_rank = 1
            ",
        );
        let mut conn = self.conn()?;
        let transaction = conn.transaction()?;
        transaction.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;
        transaction.execute(&insert_sql, params![chain, chain])?;
        transaction.execute(
            &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        transaction.commit()?;
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

    pub fn load_parquet_dataset_auto(
        &self,
        parquet_path: &str,
    ) -> Result<Vec<crate::models::Chain>, AppError> {
        self.load_parquet_datasets_auto(&[parquet_path.to_string()])
    }

    pub fn load_parquet_datasets_auto(
        &self,
        parquet_paths: &[String],
    ) -> Result<Vec<crate::models::Chain>, AppError> {
        if parquet_paths.is_empty() {
            return Ok(Vec::new());
        }
        let path_list = parquet_paths
            .iter()
            .map(|path| Self::sql_string_literal(&path.replace('\\', "/")))
            .collect::<Vec<_>>()
            .join(", ");
        let conn = self.conn()?;
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE incoming_nft_features AS
             SELECT * FROM read_parquet([{path_list}], union_by_name = true)"
        ))?;
        let columns = Self::table_columns(&conn, "incoming_nft_features")?;
        let missing = REQUIRED_SNAPSHOT_COLUMNS
            .iter()
            .copied()
            .filter(|column| !columns.contains(*column))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            conn.execute_batch("DROP TABLE incoming_nft_features")?;
            return Err(AppError::InvalidData(format!(
                "Parquet inputs are missing required snapshot columns {missing:?}"
            )));
        }
        let invalid_identity_count: i64 = conn.query_row(
            "SELECT count(*) FROM incoming_nft_features
             WHERE chain IS NULL OR trim(CAST(chain AS VARCHAR)) = ''
                OR contract_address IS NULL OR trim(CAST(contract_address AS VARCHAR)) = ''
                OR token_id IS NULL OR trim(CAST(token_id AS VARCHAR)) = ''",
            [],
            |row| row.get(0),
        )?;
        if invalid_identity_count > 0 {
            conn.execute_batch("DROP TABLE incoming_nft_features")?;
            return Err(AppError::InvalidData(format!(
                "Parquet inputs contain {invalid_identity_count} rows with missing chain, contract_address, or token_id"
            )));
        }
        let chain_names = {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT lower(trim(CAST(chain AS VARCHAR)))
                 FROM incoming_nft_features ORDER BY 1",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            values
        };
        let chains = chain_names
            .iter()
            .map(|chain| chain.parse::<crate::models::Chain>())
            .collect::<Result<Vec<_>, _>>();
        let chains = match chains {
            Ok(chains) => chains,
            Err(error) => {
                conn.execute_batch("DROP TABLE incoming_nft_features")?;
                return Err(error);
            }
        };
        drop(conn);
        for chain in &chains {
            self.invalidate_metadata_recall_index(chain.as_str())?;
        }
        let mut conn = self.conn()?;
        let transaction = conn.transaction()?;
        transaction.execute_batch(
            "CREATE OR REPLACE TEMP TABLE deduped_incoming_nft_features AS
             SELECT * EXCLUDE (source_rank) FROM (
                  SELECT
                     lower(trim(CAST(chain AS VARCHAR))) AS chain_norm,
                     CASE WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'
                          THEN trim(CAST(contract_address AS VARCHAR))
                          ELSE lower(trim(CAST(contract_address AS VARCHAR))) END
                         AS contract_address_norm,
                     CAST(token_id AS VARCHAR) AS token_id_norm,
                     coalesce(CAST(token_uri AS VARCHAR), '') AS token_uri_value,
                     coalesce(CAST(image_uri AS VARCHAR), '') AS image_uri_value,
                     coalesce(CAST(name AS VARCHAR), '') AS name_value,
                     coalesce(CAST(symbol AS VARCHAR), '') AS symbol_value,
                     coalesce(CAST(metadata_json AS VARCHAR), '') AS metadata_json_value,
                     coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm_value,
                     coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm_value,
                     coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm_value,
                      row_number() OVER (
                         PARTITION BY lower(trim(CAST(chain AS VARCHAR))),
                                      CASE WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'
                                           THEN trim(CAST(contract_address AS VARCHAR))
                                           ELSE lower(trim(CAST(contract_address AS VARCHAR))) END,
                                      CAST(token_id AS VARCHAR)
                          ORDER BY
                              ((trim(coalesce(CAST(token_uri AS VARCHAR), '')) <> '')::INTEGER
                               + (trim(coalesce(CAST(image_uri AS VARCHAR), '')) <> '')::INTEGER
                               + (trim(coalesce(CAST(name AS VARCHAR), '')) <> '')::INTEGER
                               + (trim(coalesce(CAST(metadata_json AS VARCHAR), '')) <> '')::INTEGER
                               + (trim(coalesce(CAST(token_uri_norm AS VARCHAR), '')) <> '')::INTEGER
                               + (trim(coalesce(CAST(image_uri_norm AS VARCHAR), '')) <> '')::INTEGER
                               + (trim(coalesce(CAST(name_norm AS VARCHAR), '')) <> '')::INTEGER) DESC,
                              length(coalesce(CAST(metadata_json AS VARCHAR), '')) DESC,
                              CAST(token_uri AS VARCHAR), CAST(image_uri AS VARCHAR),
                              CAST(name AS VARCHAR), CAST(metadata_json AS VARCHAR)
                      ) AS source_rank
                  FROM incoming_nft_features
              ) source
              WHERE source.source_rank = 1;

             DELETE FROM nft_features
             WHERE EXISTS (
                 SELECT 1 FROM deduped_incoming_nft_features source
                 WHERE nft_features.chain = source.chain_norm
                   AND nft_features.contract_address = source.contract_address_norm
                   AND nft_features.token_id = source.token_id_norm
             );

             INSERT INTO nft_features (
                 chain, contract_address, token_id, token_uri, image_uri, name, symbol,
                 metadata_json, token_uri_norm, image_uri_norm, name_norm
             )
             SELECT chain_norm, contract_address_norm, token_id_norm, token_uri_value,
                    image_uri_value, name_value, symbol_value, metadata_json_value,
                    token_uri_norm_value, image_uri_norm_value, name_norm_value
             FROM deduped_incoming_nft_features",
        )?;
        for chain in &chains {
            transaction.execute(
                &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
                params![chain.as_str()],
            )?;
        }
        transaction.execute_batch(
            "DROP TABLE deduped_incoming_nft_features;
             DROP TABLE incoming_nft_features",
        )?;
        transaction.commit()?;
        Ok(chains)
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
