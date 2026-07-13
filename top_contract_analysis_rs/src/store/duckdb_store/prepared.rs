use super::*;

impl DuckDbFeatureStore {
    pub fn require_prepared_for_chains(
        &self,
        chains: &[crate::models::Chain],
    ) -> Result<(), AppError> {
        let conn = self.conn()?;
        match Self::prepare_journal_state_from_connection(&conn)? {
            Some(journal) => {
                if journal.phase != "ready" {
                    return Err(AppError::InvalidData(format!(
                        "authoritative feature preparation is not globally ready (journal phase {:?}); resume prepare-features before analysis",
                        journal.phase
                    )));
                }
                Self::validate_journal_generations(&conn, &journal)?;
                let expected_chains = journal
                    .expected_chains
                    .iter()
                    .map(String::as_str)
                    .collect::<HashSet<_>>();
                for chain in chains {
                    if !expected_chains.contains(chain.as_str()) {
                        return Err(AppError::InvalidData(format!(
                            "requested chain {:?} is not part of the globally ready authoritative snapshot",
                            chain.as_str()
                        )));
                    }
                }
            }
            None if !self.writable => {
                return Err(AppError::InvalidData(
                    "authoritative prepare journal is missing; import the full snapshot with prepare-features before read-only analysis"
                        .to_string(),
                ));
            }
            None => {}
        }
        for chain in chains {
            if !Self::prepared_recall_state(&conn, chain.as_str())?.ready {
                return Err(AppError::InvalidData(format!(
                    "prepared feature generation is missing or stale for chain {:?}; run prepare-features first",
                    chain.as_str()
                )));
            }
        }
        Ok(())
    }

    pub fn prepare_recall_for_chains(
        &self,
        chains: &[crate::models::Chain],
    ) -> Result<(), AppError> {
        let result = (|| -> Result<(), AppError> {
            if !self.writable {
                return Err(AppError::InvalidData(
                    "feature preparation requires a writable database".to_string(),
                ));
            }
            for chain in chains {
                self.invalidate_metadata_recall_index(chain.as_str())?;
            }
            let mut conn = self.conn()?;
            Self::create_prepared_recall_tables(&conn)?;
            let requested_chains = chains
                .iter()
                .map(|chain| chain.as_str().to_string())
                .collect::<BTreeSet<_>>();
            let journal = Self::prepare_journal_state_from_connection(&conn)?.filter(|journal| {
                journal
                    .expected_chains
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    == requested_chains
            });
            if let Some(journal) = &journal {
                Self::validate_journal_generations(&conn, journal)?;
                if journal.phase == "ready" {
                    for chain in chains {
                        if !Self::prepared_recall_state(&conn, chain.as_str())?.ready {
                            return Err(AppError::InvalidData(format!(
                            "prepare journal is ready but prepared state is stale for chain {:?}; rerun with --restart-prepare",
                            chain.as_str()
                        )));
                        }
                    }
                    return Ok(());
                }
            }

            let indexes_already_built = if journal
                .as_ref()
                .is_some_and(|state| state.phase == "indexes_built")
            {
                Self::global_recall_indexes_exist(&conn)?
            } else {
                false
            };
            if !indexes_already_built {
                conn.execute_batch(
                    "DROP INDEX IF EXISTS nft_uri_recall_posting_idx;
                 DROP INDEX IF EXISTS nft_contract_metadata_cursor_idx",
                )?;
            }
            let mut prepared_chains = journal
                .as_ref()
                .map(|journal| journal.prepared_chains.clone())
                .unwrap_or_default();
            for chain in chains {
                if prepared_chains.contains(chain.as_str())
                    && Self::prepared_recall_state(&conn, chain.as_str())?.ready
                {
                    continue;
                }
                let transaction = conn.transaction()?;
                Self::refresh_prepared_recall_tables_for_chain_in_transaction(
                    &transaction,
                    chain.as_str(),
                )?;
                prepared_chains.insert(chain.as_str().to_string());
                if let Some(journal) = &journal {
                    Self::update_prepare_journal_phase(
                        &transaction,
                        journal,
                        &prepared_chains,
                        &format!("prepared:{}", chain.as_str()),
                    )?;
                }
                transaction.commit()?;
                conn.execute_batch("CHECKPOINT")?;
            }
            if !indexes_already_built {
                let transaction = conn.transaction()?;
                transaction.execute_batch(&format!(
                    "CREATE INDEX nft_uri_recall_posting_idx
                     ON {URI_RECALL_POSTING_TABLE}(chain, uri_kind, uri_hash);
                 CREATE INDEX nft_contract_metadata_cursor_idx
                     ON {CONTRACT_REPRESENTATIVE_TABLE}(chain, metadata_feature_rowid)"
                ))?;
                if let Some(journal) = &journal {
                    Self::update_prepare_journal_phase(
                        &transaction,
                        journal,
                        &prepared_chains,
                        "indexes_built",
                    )?;
                }
                transaction.commit()?;
                conn.execute_batch("CHECKPOINT")?;
            }
            if let Some(journal) = &journal {
                let transaction = conn.transaction()?;
                Self::update_prepare_journal_phase(
                    &transaction,
                    journal,
                    &prepared_chains,
                    "ready",
                )?;
                transaction.commit()?;
            }
            conn.execute_batch("CHECKPOINT")?;
            Ok(())
        })();
        if let Err(error) = &result {
            self.record_prepare_journal_error(error);
        }
        result
    }

    fn global_recall_indexes_exist(conn: &Connection) -> Result<bool, AppError> {
        let count = conn.query_row(
            "SELECT count(DISTINCT index_name)
             FROM duckdb_indexes()
             WHERE index_name IN ('nft_uri_recall_posting_idx', 'nft_contract_metadata_cursor_idx')",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count == 2)
    }

    fn update_prepare_journal_phase(
        conn: &Connection,
        journal: &PrepareJournalState,
        prepared_chains: &BTreeSet<String>,
        phase: &str,
    ) -> Result<(), AppError> {
        let changed = conn.execute(
            &format!(
                "UPDATE {PREPARE_JOURNAL_TABLE}
                 SET prepared_chains_json = ?, phase = ?, updated_at_ms = ?, last_error = ''
                 WHERE journal_id = 1 AND input_fingerprint = ?"
            ),
            params![
                serde_json::to_string(prepared_chains)?,
                phase,
                Self::unix_time_millis(),
                journal.input_fingerprint
            ],
        )?;
        if changed != 1 {
            return Err(AppError::InvalidData(
                "prepare journal changed during derived-table preparation".to_string(),
            ));
        }
        Ok(())
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
                           row(name_sort, token_id)
                       ) FILTER (WHERE name_eligible) AS name_feature_rowid,
                       arg_min(
                           feature_rowid,
                           token_id
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

    pub(super) fn refresh_uri_postings_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        conn.execute(
            &format!("DELETE FROM {URI_RECALL_POSTING_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        let sql = format!(
            "
            INSERT INTO {URI_RECALL_POSTING_TABLE} (
                chain, uri_kind, uri_hash, feature_rowid
            )
            SELECT
                chain,
                1::UTINYINT AS uri_kind,
                md5_number_lower(CAST(token_uri_norm AS VARCHAR)) AS uri_hash,
                rowid AS feature_rowid
            FROM nft_features
            WHERE chain = ?
              AND trim(coalesce(CAST(token_uri_norm AS VARCHAR), '')) <> ''
            UNION ALL
            SELECT
                chain,
                2::UTINYINT AS uri_kind,
                md5_number_lower(CAST(image_uri_norm AS VARCHAR)) AS uri_hash,
                rowid AS feature_rowid
            FROM nft_features
            WHERE chain = ?
              AND trim(coalesce(CAST(image_uri_norm AS VARCHAR), '')) <> ''
            "
        );
        conn.execute(&sql, params![chain, chain])?;
        Ok(())
    }

    pub(super) fn refresh_name_recall_rows_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        conn.execute(
            &format!("DELETE FROM {NAME_RECALL_ROW_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        conn.execute(
            &format!(
                "
                INSERT INTO {NAME_RECALL_ROW_TABLE} (
                    chain, feature_rowid, contract_address, name_norm
                )
                WITH eligible AS (
                    SELECT chain,
                           rowid AS feature_rowid,
                           CASE WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'
                                THEN trim(CAST(contract_address AS VARCHAR))
                                ELSE lower(trim(CAST(contract_address AS VARCHAR))) END AS contract_address,
                           CAST(token_id AS VARCHAR) AS token_id,
                           trim(CAST(name_norm AS VARCHAR)) AS name_norm
                    FROM nft_features
                    WHERE chain = ?
                      AND trim(coalesce(CAST(name_norm AS VARCHAR), '')) <> ''
                )
                SELECT chain,
                       arg_min(feature_rowid, row(token_id, feature_rowid)) AS feature_rowid,
                       contract_address,
                       name_norm
                FROM eligible
                GROUP BY chain, contract_address, name_norm
                "
            ),
            params![chain],
        )?;
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
                       coalesce(CAST(f.metadata_json AS VARCHAR), '') AS metadata_json
                FROM {CONTRACT_REPRESENTATIVE_TABLE} r
                JOIN nft_features f ON f.rowid = r.metadata_feature_rowid
                WHERE r.chain = ?
                  AND r.metadata_feature_rowid IS NOT NULL
                  AND r.metadata_feature_rowid > {last_feature_rowid}
                  AND {}
                ORDER BY r.metadata_feature_rowid
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
                    let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
                    for row_index in 0..batch.num_rows() {
                        collected.push(MetadataRecallSourceRow {
                            feature_rowid: rowid_column.value(row_index),
                            contract_address: contract_column.value(row_index).to_owned(),
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
                   coalesce(CAST(f.metadata_json AS VARCHAR), '') AS metadata_json
            FROM nft_features f
            JOIN {UNUSABLE_METADATA_CONTRACT_TABLE} u
              ON u.contract_address = CASE WHEN lower(trim(CAST(f.chain AS VARCHAR))) = 'solana'
                                           THEN trim(CAST(f.contract_address AS VARCHAR))
                                           ELSE lower(trim(CAST(f.contract_address AS VARCHAR))) END
            WHERE f.chain = ?
              AND {}
            ORDER BY contract_address, CAST(f.token_id AS VARCHAR)
            ",
            Self::sql_metadata_json_eligible_predicate("f.metadata_json")
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut resolved_contracts = HashSet::new();
        let mut prepared_rows = Vec::new();
        for batch in stmt.query_arrow(params![chain])? {
            let rowid_column = arrow_i64_column(&batch, 0, "feature_rowid")?;
            let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
            let metadata_column = arrow_string_column(&batch, 2, "metadata_json")?;
            for row_index in 0..batch.num_rows() {
                let contract_address = contract_column.value(row_index);
                if resolved_contracts.contains(contract_address) {
                    continue;
                }
                let source = MetadataRecallSourceRow {
                    feature_rowid: rowid_column.value(row_index),
                    contract_address: contract_address.to_owned(),
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
        conn.execute(
            &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        Self::refresh_contract_representatives_for_chain(conn, chain)?;
        Self::refresh_name_recall_rows_for_chain(conn, chain)?;
        Self::refresh_uri_postings_for_chain(conn, chain)?;
        Self::refresh_metadata_recall_docs_for_chain(conn, chain)?;
        let generation = Self::feature_generation_state(conn, chain)?.ok_or_else(|| {
            AppError::InvalidData(format!(
                "feature generation identity is missing for chain {chain:?}; import the chain before preparing it"
            ))
        })?;
        conn.execute(
            &format!(
                "
                INSERT INTO {PREPARED_RECALL_CHAIN_TABLE} (
                    chain, feature_generation_id, feature_row_count,
                    max_feature_rowid, feature_fingerprint,
                    prepared_format_version, normalization_version,
                    recall_algorithm_version, report_schema_version, build_fingerprint
                ) VALUES (?, ?, ?, -1, ?, ?, ?, ?, ?, ?)
                "
            ),
            params![
                chain,
                generation.generation_id,
                generation.row_count,
                generation.generation_id,
                PREPARED_FORMAT_VERSION,
                NORMALIZATION_VERSION,
                RECALL_ALGORITHM_VERSION,
                REPORT_SCHEMA_VERSION,
                BUILD_FINGERPRINT
            ],
        )?;
        Ok(())
    }

    pub(super) fn refresh_prepared_recall_tables_for_chain(
        conn: &Connection,
        chain: &str,
    ) -> Result<(), AppError> {
        Self::create_prepared_recall_tables(conn)?;
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
        if state.ready {
            return Ok(state);
        }
        let mut generation = Self::feature_generation_state(conn, chain)?;
        if generation.is_none() {
            let has_rows = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM nft_features WHERE chain = ? LIMIT 1)",
                params![chain],
                |row| row.get::<_, bool>(0),
            )?;
            if !has_rows {
                return Ok(state);
            }
            if self.writable {
                generation = Some(Self::record_feature_generation(conn, chain)?);
            }
        }
        if self.writable {
            self.invalidate_metadata_recall_index(chain)?;
            Self::refresh_prepared_recall_tables_for_chain(conn, chain)?;
            state = Self::prepared_recall_state(conn, chain)?;
        }
        if !state.ready {
            return Err(AppError::InvalidData(format!(
                "prepared recall tables are missing or stale for chain {chain:?}; run prepare-features first"
            )));
        }
        debug_assert!(generation.is_some());
        Ok(state)
    }
}
