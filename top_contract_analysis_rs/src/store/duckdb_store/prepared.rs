use super::*;

impl DuckDbFeatureStore {
    pub(super) fn feature_chain_stats(
        conn: &Connection,
        chain: &str,
    ) -> Result<FeatureChainStats, AppError> {
        conn.query_row(
            "
            SELECT
                CAST(count(*) AS BIGINT) AS row_count,
                CAST(coalesce(max(rowid), -1) AS BIGINT) AS max_feature_rowid,
                CAST(coalesce(sum(rowid), -1) AS VARCHAR) AS feature_fingerprint
            FROM nft_features
            WHERE chain = ?
            ",
            params![chain],
            |row| {
                Ok(FeatureChainStats {
                    row_count: row.get(0)?,
                    max_feature_rowid: row.get(1)?,
                    fingerprint: row.get(2)?,
                })
            },
        )
        .map_err(AppError::from)
    }

    pub(super) fn prepared_recall_chain_stats(
        conn: &Connection,
        chain: &str,
    ) -> Result<Option<FeatureChainStats>, AppError> {
        let mut stmt = conn.prepare(&format!(
            "
            SELECT
                coalesce(feature_row_count, -1),
                coalesce(max_feature_rowid, -1),
                coalesce(feature_fingerprint, '')
            FROM {PREPARED_RECALL_CHAIN_TABLE}
            WHERE chain = ?
            LIMIT 1
            "
        ))?;
        let mut rows = stmt.query_map(params![chain], |row| {
            Ok(FeatureChainStats {
                row_count: row.get(0)?,
                max_feature_rowid: row.get(1)?,
                fingerprint: row.get(2)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub(super) fn refresh_contract_representatives_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        Self::create_contract_representative_table(conn)?;
        conn.execute(
            &format!("DELETE FROM {CONTRACT_REPRESENTATIVE_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        conn.execute(&Self::contract_representatives_insert_sql(), params![chain])?;
        Ok(())
    }

    pub(super) fn contract_representatives_insert_sql() -> String {
        let sql = format!(
            "
            INSERT INTO {CONTRACT_REPRESENTATIVE_TABLE} (
                chain, contract_address, name_feature_rowid, metadata_feature_rowid
            )
            WITH feature_rows AS (
                SELECT chain,
                       CASE WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'
                            THEN trim(CAST(contract_address AS VARCHAR))
                            ELSE lower(trim(CAST(contract_address AS VARCHAR))) END AS contract_address,
                       rowid AS feature_rowid,
                       CAST(token_id AS VARCHAR) AS token_id,
                       trim(coalesce(CAST(name AS VARCHAR), '')) AS name_sort,
                       trim(coalesce(CAST(name_norm AS VARCHAR), '')) <> '' AS name_eligible,
                       {} AS metadata_eligible
                FROM nft_features
                WHERE chain = ?
            ),
            representatives AS (
                SELECT chain,
                       contract_address,
                       arg_min(
                           feature_rowid,
                           row(name_sort, feature_rowid)
                       ) FILTER (WHERE name_eligible) AS name_feature_rowid,
                       arg_min(
                           feature_rowid,
                           row(token_id, feature_rowid)
                       ) FILTER (WHERE metadata_eligible) AS metadata_feature_rowid
                FROM feature_rows
                GROUP BY chain, contract_address
            )
            SELECT chain,
                   contract_address,
                   name_feature_rowid,
                   metadata_feature_rowid
            FROM representatives
            WHERE name_feature_rowid IS NOT NULL
               OR metadata_feature_rowid IS NOT NULL
            ",
            Self::sql_metadata_json_eligible_predicate("metadata_json")
        );
        sql
    }

    pub(super) fn refresh_feature_recall_rows_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        conn.execute(
            &format!("DELETE FROM {FEATURE_RECALL_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        let sql = format!(
            "
            INSERT INTO {FEATURE_RECALL_TABLE} (
                chain, feature_rowid, contract_address, token_id, token_uri_norm,
                image_uri_norm, name_norm, name_sort
            )
            SELECT
                chain,
                rowid AS feature_rowid,
                CASE WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'
                     THEN trim(CAST(contract_address AS VARCHAR))
                     ELSE lower(trim(CAST(contract_address AS VARCHAR))) END AS contract_address,
                CAST(token_id AS VARCHAR) AS token_id,
                coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm,
                trim(coalesce(CAST(name AS VARCHAR), '')) AS name_sort
            FROM nft_features
            WHERE chain = ?
            "
        );
        conn.execute(&sql, params![chain])?;
        Ok(())
    }

    pub(super) fn refresh_metadata_recall_docs_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        conn.execute(
            &format!("DELETE FROM {METADATA_RECALL_DOC_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        let mut last_feature_rowid = -1_i64;
        let mut unusable_contracts = HashSet::new();
        loop {
            let sql = format!(
                "
                SELECT f.rowid AS feature_rowid,
                       CASE WHEN lower(trim(CAST(f.chain AS VARCHAR))) = 'solana'
                            THEN trim(CAST(f.contract_address AS VARCHAR))
                            ELSE lower(trim(CAST(f.contract_address AS VARCHAR))) END AS contract_address,
                       CAST(f.token_id AS VARCHAR) AS token_id,
                       coalesce(CAST(f.token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                       coalesce(CAST(f.image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                       coalesce(CAST(f.name_norm AS VARCHAR), '') AS name_norm,
                       coalesce(CAST(f.metadata_json AS VARCHAR), '') AS metadata_json
                FROM {CONTRACT_REPRESENTATIVE_TABLE} r
                JOIN nft_features f ON f.rowid = r.metadata_feature_rowid
                WHERE r.chain = ?
                  AND r.metadata_feature_rowid IS NOT NULL
                  AND f.rowid > {last_feature_rowid}
                  AND {}
                ORDER BY f.rowid
                LIMIT {PREPARED_METADATA_DOC_BATCH_SIZE}
                ",
                Self::sql_metadata_json_eligible_predicate("f.metadata_json")
            );
            let rows = {
                let mut stmt = conn.prepare(&sql)?;
                let mut collected = Vec::new();
                for batch in stmt.query_arrow(params![chain])? {
                    let rowid_column = arrow_i64_column(&batch, 0, "feature_rowid")?;
                    let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
                    let token_column = arrow_string_column(&batch, 2, "token_id")?;
                    let token_uri_column = arrow_string_column(&batch, 3, "token_uri_norm")?;
                    let image_uri_column = arrow_string_column(&batch, 4, "image_uri_norm")?;
                    let name_column = arrow_string_column(&batch, 5, "name_norm")?;
                    let metadata_column = arrow_string_column(&batch, 6, "metadata_json")?;
                    for row_index in 0..batch.num_rows() {
                        collected.push(MetadataRecallSourceRow {
                            feature_rowid: rowid_column.value(row_index),
                            contract_address: contract_column.value(row_index).to_owned(),
                            token_id: token_column.value(row_index).to_owned(),
                            token_uri_norm: token_uri_column.value(row_index).to_owned(),
                            image_uri_norm: image_uri_column.value(row_index).to_owned(),
                            name_norm: name_column.value(row_index).to_owned(),
                            metadata_json: metadata_column.value(row_index).to_owned(),
                        });
                    }
                }
                collected
            };
            if rows.is_empty() {
                break;
            }
            let fetched_rows = rows.len();
            last_feature_rowid = rows
                .last()
                .map_or(last_feature_rowid, |row| row.feature_rowid);
            let prepared_results = rows
                .into_par_iter()
                .map(MetadataRecallSourceRow::prepare)
                .collect::<Vec<_>>();
            let mut prepared_rows = Vec::with_capacity(prepared_results.len());
            for prepared in prepared_results {
                match prepared {
                    Ok(row) => prepared_rows.push(row),
                    Err(contract_address) => {
                        unusable_contracts.insert(contract_address);
                    }
                }
            }
            Self::append_prepared_metadata_recall_rows(conn, chain, &prepared_rows)?;
            if fetched_rows < PREPARED_METADATA_DOC_BATCH_SIZE {
                break;
            }
        }
        if !unusable_contracts.is_empty() {
            let fallback_rows =
                Self::resolve_metadata_representative_fallbacks(conn, chain, &unusable_contracts)?;
            Self::append_prepared_metadata_recall_rows(conn, chain, &fallback_rows)?;
        }
        Ok(())
    }

    pub(super) fn append_prepared_metadata_recall_rows(
        conn: &Connection,
        chain: &str,
        rows: &[PreparedMetadataRecallRow],
    ) -> Result<(), AppError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut appender = conn.appender(METADATA_RECALL_DOC_TABLE)?;
        for row in rows {
            appender.append_row(params![
                chain,
                row.feature_rowid,
                row.contract_address,
                row.token_id,
                row.token_uri_norm,
                row.image_uri_norm,
                row.name_norm,
                row.recall_doc
            ])?;
        }
        appender.flush()?;
        Ok(())
    }

    pub(super) fn resolve_metadata_representative_fallbacks(
        conn: &Connection,
        chain: &str,
        unusable_contracts: &HashSet<String>,
    ) -> Result<Vec<PreparedMetadataRecallRow>, AppError> {
        let result =
            Self::resolve_metadata_representative_fallbacks_inner(conn, chain, unusable_contracts);
        // Always drop the session-scoped scratch temp tables, even when the
        // inner work fails partway, so a partial failure cannot leave them
        // behind for later calls on the same connection.
        let _ = conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {UNUSABLE_METADATA_CONTRACT_TABLE};
            DROP TABLE IF EXISTS {RESOLVED_METADATA_REP_TABLE};
            "
        ));
        result
    }

    pub(super) fn resolve_metadata_representative_fallbacks_inner(
        conn: &Connection,
        chain: &str,
        unusable_contracts: &HashSet<String>,
    ) -> Result<Vec<PreparedMetadataRecallRow>, AppError> {
        conn.execute_batch(&format!(
            "
            CREATE OR REPLACE TEMP TABLE {UNUSABLE_METADATA_CONTRACT_TABLE} (
                contract_address VARCHAR PRIMARY KEY
            );
            "
        ))?;
        {
            let mut appender = conn.appender(UNUSABLE_METADATA_CONTRACT_TABLE)?;
            for contract_address in unusable_contracts {
                appender.append_row([contract_address])?;
            }
            appender.flush()?;
        }

        let sql = format!(
            "
            SELECT f.rowid AS feature_rowid,
                   CASE WHEN lower(trim(CAST(f.chain AS VARCHAR))) = 'solana'
                        THEN trim(CAST(f.contract_address AS VARCHAR))
                        ELSE lower(trim(CAST(f.contract_address AS VARCHAR))) END AS contract_address,
                   CAST(f.token_id AS VARCHAR) AS token_id,
                   coalesce(CAST(f.token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                   coalesce(CAST(f.image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                   coalesce(CAST(f.name_norm AS VARCHAR), '') AS name_norm,
                   coalesce(CAST(f.metadata_json AS VARCHAR), '') AS metadata_json
            FROM nft_features f
            JOIN {UNUSABLE_METADATA_CONTRACT_TABLE} u
              ON u.contract_address = CASE WHEN lower(trim(CAST(f.chain AS VARCHAR))) = 'solana'
                                           THEN trim(CAST(f.contract_address AS VARCHAR))
                                           ELSE lower(trim(CAST(f.contract_address AS VARCHAR))) END
            WHERE f.chain = ?
              AND {}
            ORDER BY contract_address, token_id, f.rowid
            ",
            Self::sql_metadata_json_eligible_predicate("f.metadata_json")
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut resolved_contracts = HashSet::new();
        let mut prepared_rows = Vec::new();
        for batch in stmt.query_arrow(params![chain])? {
            let rowid_column = arrow_i64_column(&batch, 0, "feature_rowid")?;
            let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
            let token_column = arrow_string_column(&batch, 2, "token_id")?;
            let token_uri_column = arrow_string_column(&batch, 3, "token_uri_norm")?;
            let image_uri_column = arrow_string_column(&batch, 4, "image_uri_norm")?;
            let name_column = arrow_string_column(&batch, 5, "name_norm")?;
            let metadata_column = arrow_string_column(&batch, 6, "metadata_json")?;
            for row_index in 0..batch.num_rows() {
                let contract_address = contract_column.value(row_index);
                if resolved_contracts.contains(contract_address) {
                    continue;
                }
                let source = MetadataRecallSourceRow {
                    feature_rowid: rowid_column.value(row_index),
                    contract_address: contract_address.to_owned(),
                    token_id: token_column.value(row_index).to_owned(),
                    token_uri_norm: token_uri_column.value(row_index).to_owned(),
                    image_uri_norm: image_uri_column.value(row_index).to_owned(),
                    name_norm: name_column.value(row_index).to_owned(),
                    metadata_json: metadata_column.value(row_index).to_owned(),
                };
                if let Ok(prepared) = source.prepare() {
                    resolved_contracts.insert(prepared.contract_address.clone());
                    prepared_rows.push(prepared);
                }
            }
        }
        drop(stmt);

        if !prepared_rows.is_empty() {
            conn.execute_batch(&format!(
                "
                CREATE OR REPLACE TEMP TABLE {RESOLVED_METADATA_REP_TABLE} (
                    contract_address VARCHAR PRIMARY KEY,
                    feature_rowid BIGINT NOT NULL
                );
                "
            ))?;
            {
                let mut appender = conn.appender(RESOLVED_METADATA_REP_TABLE)?;
                for row in &prepared_rows {
                    appender.append_row(params![row.contract_address, row.feature_rowid])?;
                }
                appender.flush()?;
            }
            conn.execute(
                &format!(
                    "
                    UPDATE {CONTRACT_REPRESENTATIVE_TABLE} r
                    SET metadata_feature_rowid = resolved.feature_rowid
                    FROM {RESOLVED_METADATA_REP_TABLE} resolved
                    WHERE r.chain = ?
                      AND r.contract_address = resolved.contract_address
                    "
                ),
                params![chain],
            )?;
        }
        Ok(prepared_rows)
    }

    pub(super) fn refresh_prepared_recall_tables_for_chain_in_transaction(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        Self::create_prepared_recall_tables(conn)?;
        conn.execute(
            &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        Self::refresh_contract_representatives_for_chain(conn, chain)?;
        Self::refresh_feature_recall_rows_for_chain(conn, chain)?;
        Self::refresh_metadata_recall_docs_for_chain(conn, chain)?;
        let stats = Self::feature_chain_stats(conn, chain)?;
        conn.execute(
            &format!(
                "
                INSERT INTO {PREPARED_RECALL_CHAIN_TABLE} (
                    chain, feature_row_count, max_feature_rowid, feature_fingerprint
                ) VALUES (?, ?, ?, ?)
                "
            ),
            params![
                chain,
                stats.row_count,
                stats.max_feature_rowid,
                stats.fingerprint
            ],
        )?;
        Ok(())
    }

    pub(super) fn refresh_prepared_recall_tables_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        let transaction = conn.unchecked_transaction()?;
        Self::refresh_prepared_recall_tables_for_chain_in_transaction(&transaction, chain)?;
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn ensure_prepared_recall_state(
        &self,
        conn: &Connection,
        chain: &str,
    ) -> Result<PreparedRecallState, AppError> {
        let mut state = Self::prepared_recall_state(conn, chain)?;
        let stats = Self::feature_chain_stats(conn, chain)?;
        if state.ready || stats.row_count == 0 {
            return Ok(state);
        }
        if self.writable {
            self.invalidate_metadata_recall_index(chain)?;
            Self::refresh_prepared_recall_tables_for_chain(conn, chain)?;
            state = Self::prepared_recall_state(conn, chain)?;
        }
        if !state.ready {
            return Err(AppError::InvalidData(format!(
                "prepared recall tables are missing or stale for chain {chain:?}; reopen the feature DB in writable mode or rebuild the snapshot"
            )));
        }
        Ok(state)
    }
}
