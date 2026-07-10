use super::*;

impl DuckDbFeatureStore {
    pub(super) fn prepare_seed_value_table(
        conn: &Connection,
        table_name: &str,
        values: &HashSet<String>,
    ) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {table_name};
            CREATE TEMP TABLE {table_name} (
                value VARCHAR NOT NULL
            );
            "
        ))?;
        if values.is_empty() {
            return Ok(());
        }

        let mut stmt = conn.prepare(&format!("INSERT INTO {table_name} (value) VALUES (?)"))?;
        let values = values
            .iter()
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>();
        for value in values {
            stmt.execute(params![value])?;
        }
        Ok(())
    }

    pub(super) fn drop_seed_temp_tables(conn: &Connection) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {SEED_TOKEN_URI_TABLE};
            DROP TABLE IF EXISTS {SEED_IMAGE_URI_TABLE};
            DROP TABLE IF EXISTS {SEED_TOKEN_ID_TABLE};
            DROP TABLE IF EXISTS {CANDIDATE_CONTRACT_TABLE};
            DROP TABLE IF EXISTS {SELECTED_RECALL_ROWID_TABLE};
            "
        ))?;
        Ok(())
    }

    pub(super) fn create_selected_recall_rowid_table(conn: &Connection) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "
            DROP TABLE IF EXISTS {SELECTED_RECALL_ROWID_TABLE};
            CREATE TEMP TABLE {SELECTED_RECALL_ROWID_TABLE} (
                feature_rowid BIGINT NOT NULL
            );
            "
        ))?;
        Ok(())
    }

    pub(super) fn replace_selected_recall_rowids(conn: &Connection, rowids: &[i64]) -> Result<(), AppError> {
        conn.execute(&format!("DELETE FROM {SELECTED_RECALL_ROWID_TABLE}"), [])?;
        let unique_rowids = rowids.iter().copied().collect::<BTreeSet<_>>();
        for chunk in unique_rowids
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(SELECTED_RECALL_ROWID_VALUES_CHUNK_SIZE)
        {
            if chunk.is_empty() {
                continue;
            }
            let values = chunk
                .iter()
                .map(|rowid| format!("({rowid})"))
                .collect::<Vec<_>>()
                .join(",");
            conn.execute(
                &format!(
                    "INSERT INTO {SELECTED_RECALL_ROWID_TABLE} (feature_rowid) VALUES {values}"
                ),
                [],
            )?;
        }
        Ok(())
    }

    pub(super) fn seed_row_match(
        profile: &SeedRecallProfile,
        row: &RecallRow,
        name_threshold: f64,
        metadata_recall_match: bool,
    ) -> SeedRowMatch {
        let name_match = profile.seed_name_queries.iter().any(|query| {
            query
                .score_percent(&row.name_norm, name_threshold)
                .is_some()
        });
        SeedRowMatch {
            token_uri_match: profile.exact_token_keys.contains(&row.token_uri_norm),
            image_uri_match: profile.exact_image_keys.contains(&row.image_uri_norm),
            name_prefix_match: !row.name_norm.is_empty() && name_match,
            metadata_recall_match,
        }
    }

    pub(super) fn can_select_recall_row(
        seed_index: usize,
        profile: &SeedRecallProfile,
        row: &RecallRow,
        accumulators: &BTreeMap<String, SnapshotAccumulator>,
        pending_contract_counts: &HashMap<(usize, String), usize>,
        max_tokens_per_contract: usize,
    ) -> bool {
        if profile.seed_contracts.contains(&row.contract_address) {
            return false;
        }
        let Some(accumulator) = accumulators.get(&profile.seed_address) else {
            return false;
        };
        if accumulator.seen_feature_rowids.contains(&row.feature_rowid) {
            return false;
        }
        if max_tokens_per_contract == 0 {
            return true;
        }
        let accepted = accumulator
            .per_contract_counts
            .get(&row.contract_address)
            .copied()
            .unwrap_or_default();
        let pending = pending_contract_counts
            .get(&(seed_index, row.contract_address.clone()))
            .copied()
            .unwrap_or_default();
        accepted + pending < max_tokens_per_contract
    }

    pub(super) fn note_pending_recall_row(
        seed_index: usize,
        row: &RecallRow,
        pending_contract_counts: &mut HashMap<(usize, String), usize>,
    ) {
        *pending_contract_counts
            .entry((seed_index, row.contract_address.clone()))
            .or_default() += 1;
    }

    pub(super) fn can_stage_metadata_recall_row(
        profile: &SeedRecallProfile,
        row: &RecallRow,
        accumulators: &BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> bool {
        if profile.seed_contracts.contains(&row.contract_address) {
            return false;
        }
        let Some(accumulator) = accumulators.get(&profile.seed_address) else {
            return false;
        };
        if accumulator.seen_feature_rowids.contains(&row.feature_rowid) {
            return false;
        }
        let accepted = accumulator
            .per_contract_counts
            .get(&row.contract_address)
            .copied()
            .unwrap_or_default();
        accepted < max_tokens_per_contract
    }

    pub(super) fn stage_metadata_recall_row(
        pending_rows: &mut HashMap<(usize, String), Vec<PendingMetadataRecallRow>>,
        seed_index: usize,
        row: &RecallRow,
        row_match: SeedRowMatch,
        max_tokens_per_contract: usize,
    ) {
        if max_tokens_per_contract == 0 {
            return;
        }
        let entry = pending_rows
            .entry((seed_index, row.contract_address.clone()))
            .or_default();
        if entry
            .iter()
            .any(|pending| pending.row.feature_rowid == row.feature_rowid)
        {
            return;
        }
        entry.push(PendingMetadataRecallRow {
            seed_index,
            row: row.clone(),
            row_match,
        });
        entry.sort_by(|left, right| {
            (&left.row.token_id, left.row.feature_rowid)
                .cmp(&(&right.row.token_id, right.row.feature_rowid))
        });
        entry.truncate(max_tokens_per_contract);
    }

    pub(super) fn drain_selected_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        batch_rows: &[RecallRow],
        selected_rows: &mut Vec<SelectedRecallRow>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if selected_rows.is_empty() {
            return Ok(());
        }

        let selected_rowids = selected_rows
            .iter()
            .map(|selected| batch_rows[selected.row_index].feature_rowid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let full_records_by_rowid = Self::fetch_records_by_feature_rowid(conn, &selected_rowids)?;

        for selected in selected_rows.drain(..) {
            let Some(row) = batch_rows.get(selected.row_index) else {
                continue;
            };
            let Some(record) = full_records_by_rowid.get(&row.feature_rowid) else {
                continue;
            };
            let Some(profile) = profiles.get(selected.seed_index) else {
                continue;
            };
            let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                continue;
            };
            accumulator.push_recall_row(
                profile,
                row,
                record.clone(),
                &selected.row_match,
                max_tokens_per_contract,
            );
        }

        Ok(())
    }

    pub(super) fn drain_pending_metadata_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        pending_rows: &mut HashMap<(usize, String), Vec<PendingMetadataRecallRow>>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if pending_rows.is_empty() {
            return Ok(());
        }

        let mut rows = pending_rows
            .drain()
            .flat_map(|(_, rows)| rows)
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (
                left.seed_index,
                &left.row.contract_address,
                &left.row.token_id,
                left.row.feature_rowid,
            )
                .cmp(&(
                    right.seed_index,
                    &right.row.contract_address,
                    &right.row.token_id,
                    right.row.feature_rowid,
                ))
        });

        for chunk in rows.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            let rowids = chunk
                .iter()
                .map(|pending| pending.row.feature_rowid)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let full_records_by_rowid = Self::fetch_records_by_feature_rowid(conn, &rowids)?;
            for pending in chunk {
                let Some(record) = full_records_by_rowid.get(&pending.row.feature_rowid) else {
                    continue;
                };
                let Some(profile) = profiles.get(pending.seed_index) else {
                    continue;
                };
                let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                    continue;
                };
                accumulator.push_recall_row(
                    profile,
                    &pending.row,
                    record.clone(),
                    &pending.row_match,
                    max_tokens_per_contract,
                );
            }
        }

        Ok(())
    }

    pub(super) fn drain_owned_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        rows: &mut Vec<PendingMetadataRecallRow>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if rows.is_empty() {
            return Ok(());
        }
        rows.sort_by(|left, right| {
            (
                left.seed_index,
                &left.row.contract_address,
                &left.row.token_id,
                left.row.feature_rowid,
            )
                .cmp(&(
                    right.seed_index,
                    &right.row.contract_address,
                    &right.row.token_id,
                    right.row.feature_rowid,
                ))
        });
        for chunk in rows.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            let rowids = chunk
                .iter()
                .map(|pending| pending.row.feature_rowid)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let full_records_by_rowid = Self::fetch_records_by_feature_rowid(conn, &rowids)?;
            for pending in chunk {
                let Some(record) = full_records_by_rowid.get(&pending.row.feature_rowid) else {
                    continue;
                };
                let Some(profile) = profiles.get(pending.seed_index) else {
                    continue;
                };
                let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                    continue;
                };
                accumulator.push_recall_row(
                    profile,
                    &pending.row,
                    record.clone(),
                    &pending.row_match,
                    max_tokens_per_contract,
                );
            }
        }
        rows.clear();
        Ok(())
    }

    pub(super) fn fetch_records_by_feature_rowid(
        conn: &Connection,
        rowids: &[i64],
    ) -> Result<HashMap<i64, DatabaseNftRecord>, AppError> {
        if rowids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut records = HashMap::new();
        for chunk in rowids.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            Self::replace_selected_recall_rowids(conn, chunk)?;
            let sql = format!(
                "
                SELECT f.rowid AS feature_rowid,
                       f.contract_address AS contract_address,
                       f.token_id AS token_id,
                       coalesce(f.token_uri, '') AS token_uri,
                       coalesce(f.image_uri, '') AS image_uri,
                       coalesce(f.name, '') AS name,
                       coalesce(f.symbol, '') AS symbol,
                       coalesce(f.metadata_json, '') AS metadata_json
                FROM nft_features f
                JOIN {SELECTED_RECALL_ROWID_TABLE} selected
                  ON selected.feature_rowid = f.rowid
                ORDER BY f.rowid
                "
            );
            let mut stmt = conn.prepare(&sql)?;
            for batch in stmt.query_arrow([])? {
                let rowid_column = arrow_i64_column(&batch, 0, "feature_rowid")?;
                let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
                let token_column = arrow_string_column(&batch, 2, "token_id")?;
                let token_uri_column = arrow_string_column(&batch, 3, "token_uri")?;
                let image_uri_column = arrow_string_column(&batch, 4, "image_uri")?;
                let name_column = arrow_string_column(&batch, 5, "name")?;
                let symbol_column = arrow_string_column(&batch, 6, "symbol")?;
                let metadata_column = arrow_string_column(&batch, 7, "metadata_json")?;
                for row_index in 0..batch.num_rows() {
                    records.insert(
                        rowid_column.value(row_index),
                        DatabaseNftRecord {
                            contract_address: contract_column.value(row_index).to_owned(),
                            token_id: token_column.value(row_index).to_owned(),
                            token_uri: token_uri_column.value(row_index).to_owned(),
                            image_uri: image_uri_column.value(row_index).to_owned(),
                            name: name_column.value(row_index).to_owned(),
                            symbol: symbol_column.value(row_index).to_owned(),
                            metadata_json: metadata_column.value(row_index).to_owned(),
                            metadata_recall_checked: false,
                            metadata_recall_match: false,
                        },
                    );
                }
            }
        }
        Ok(records)
    }

    pub(super) fn append_exact_uri_recall_rows(
        conn: &Connection,
        input: ExactUriRecallInput<'_>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
    ) -> Result<(), AppError> {
        let ExactUriRecallInput {
            chain,
            profiles,
            profile_index,
            all_token_keys,
            all_image_keys,
            name_threshold,
            max_tokens_per_contract,
            max_recall_rows,
            prepared_recall_state,
        } = input;
        if !prepared_recall_state.ready {
            return Ok(());
        }
        if all_token_keys.is_empty() && all_image_keys.is_empty() {
            return Ok(());
        }

        Self::prepare_seed_value_table(conn, SEED_TOKEN_URI_TABLE, all_token_keys)?;
        Self::prepare_seed_value_table(conn, SEED_IMAGE_URI_TABLE, all_image_keys)?;

        let token_uri_match_expr = if all_token_keys.is_empty() {
            "FALSE".to_string()
        } else {
            format!("token_uri_norm IN (SELECT value FROM {SEED_TOKEN_URI_TABLE})")
        };
        let image_uri_match_expr = if all_image_keys.is_empty() {
            "FALSE".to_string()
        } else {
            format!("image_uri_norm IN (SELECT value FROM {SEED_IMAGE_URI_TABLE})")
        };
        let exact_uri_predicate = format!("({token_uri_match_expr}) OR ({image_uri_match_expr})");
        let recall_batch_size = if max_recall_rows == 0 {
            DEFAULT_RECALL_BATCH_SIZE
        } else {
            max_recall_rows
        };

        let mut last_feature_rowid = -1_i64;
        loop {
            let select_sql = format!(
                "
                    SELECT feature_rowid,
                           contract_address,
                           token_id,
                           coalesce(token_uri_norm, '') AS token_uri_norm,
                           coalesce(image_uri_norm, '') AS image_uri_norm,
                           coalesce(name_norm, '') AS name_norm
                    FROM {FEATURE_RECALL_TABLE}
                    WHERE chain = ?
                      AND ({exact_uri_predicate})
                      AND feature_rowid > {last_feature_rowid}
                    ORDER BY feature_rowid
                    LIMIT {recall_batch_size}
                    "
            );
            let mut stmt = conn.prepare(&select_sql)?;
            let mut batch_rows = Vec::new();
            for batch in stmt.query_arrow(params![chain])? {
                append_recall_rows_from_arrow_batch(&batch, false, &mut batch_rows)?;
            }
            let fetched_rows = batch_rows.len();
            let last_seen_feature_rowid = batch_rows
                .last()
                .map_or(last_feature_rowid, |row| row.feature_rowid);
            drop(stmt);

            batch_rows.sort_by(|left, right| {
                (&left.contract_address, &left.token_id, left.feature_rowid).cmp(&(
                    &right.contract_address,
                    &right.token_id,
                    right.feature_rowid,
                ))
            });

            let mut selected_rows = Vec::with_capacity(SELECTED_RECALL_ROWID_CHUNK_SIZE);
            let mut pending_contract_counts = HashMap::new();
            for (row_index, row) in batch_rows.iter().enumerate() {
                let mut seed_indices = profile_index.strong_match_profiles(row);
                seed_indices.sort_unstable();
                for seed_index in seed_indices {
                    let Some(profile) = profiles.get(seed_index) else {
                        continue;
                    };
                    let row_match = Self::seed_row_match(profile, row, name_threshold, false);
                    if !(row_match.token_uri_match || row_match.image_uri_match) {
                        continue;
                    }
                    if !Self::can_select_recall_row(
                        seed_index,
                        profile,
                        row,
                        accumulators,
                        &pending_contract_counts,
                        max_tokens_per_contract,
                    ) {
                        continue;
                    }

                    selected_rows.push(SelectedRecallRow {
                        seed_index,
                        row_index,
                        row_match,
                    });
                    Self::note_pending_recall_row(seed_index, row, &mut pending_contract_counts);
                    if selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                        Self::drain_selected_recall_rows(
                            conn,
                            profiles,
                            &batch_rows,
                            &mut selected_rows,
                            accumulators,
                            max_tokens_per_contract,
                        )?;
                        pending_contract_counts.clear();
                    }
                }
            }
            Self::drain_selected_recall_rows(
                conn,
                profiles,
                &batch_rows,
                &mut selected_rows,
                accumulators,
                max_tokens_per_contract,
            )?;

            if fetched_rows < recall_batch_size {
                break;
            }
            last_feature_rowid = last_seen_feature_rowid;
        }

        Ok(())
    }

    pub(super) fn append_name_recall_rows(
        conn: &Connection,
        chain: &str,
        profiles: &[SeedRecallProfile],
        name_threshold: f64,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<(), AppError> {
        if !prepared_recall_state.ready {
            return Ok(());
        }
        if name_threshold > 100.0
            || !profiles
                .iter()
                .any(|profile| !profile.seed_name_norms.is_empty())
        {
            return Ok(());
        }
        let sql = format!(
            "
                SELECT rr.feature_rowid,
                       rr.contract_address,
                       rr.token_id,
                       coalesce(rr.token_uri_norm, '') AS token_uri_norm,
                       coalesce(rr.image_uri_norm, '') AS image_uri_norm,
                       coalesce(rr.name_norm, '') AS name_norm
                FROM {CONTRACT_REPRESENTATIVE_TABLE} r
                JOIN {FEATURE_RECALL_TABLE} rr
                  ON rr.chain = r.chain
                 AND rr.feature_rowid = r.name_feature_rowid
                WHERE r.chain = ?
                  AND r.name_feature_rowid IS NOT NULL
                  AND trim(coalesce(rr.name_norm, '')) <> ''
                ORDER BY rr.feature_rowid
                "
        );
        let mut stmt = conn.prepare(&sql)?;

        let mut selected_rows = Vec::new();
        let mut pending_contract_counts = HashMap::new();
        let mut batch_rows = Vec::with_capacity(SELECTED_RECALL_ROWID_CHUNK_SIZE);
        {
            let mut append_state = NameRecallAppendState {
                conn,
                profiles,
                selected_rows: &mut selected_rows,
                pending_contract_counts: &mut pending_contract_counts,
                accumulators,
                max_tokens_per_contract,
            };
            for batch in stmt.query_arrow(params![chain])? {
                append_recall_rows_from_arrow_batch(&batch, false, &mut batch_rows)?;
                while batch_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                    let remaining = batch_rows.split_off(SELECTED_RECALL_ROWID_CHUNK_SIZE);
                    Self::append_scored_name_recall_rows(
                        &mut append_state,
                        &batch_rows,
                        name_threshold,
                    )?;
                    batch_rows = remaining;
                }
            }
            Self::append_scored_name_recall_rows(&mut append_state, &batch_rows, name_threshold)?;
        }
        Self::drain_owned_recall_rows(
            conn,
            profiles,
            &mut selected_rows,
            accumulators,
            max_tokens_per_contract,
        )
    }

    pub(super) fn append_scored_name_recall_rows(
        state: &mut NameRecallAppendState<'_>,
        batch_rows: &[RecallRow],
        name_threshold: f64,
    ) -> Result<(), AppError> {
        if batch_rows.is_empty() {
            return Ok(());
        }
        for matched in Self::score_name_recall_batch(state.profiles, batch_rows, name_threshold) {
            let row = &batch_rows[matched.row_index];
            let profile = &state.profiles[matched.seed_index];
            if !Self::can_select_recall_row(
                matched.seed_index,
                profile,
                row,
                &*state.accumulators,
                &*state.pending_contract_counts,
                state.max_tokens_per_contract,
            ) {
                continue;
            }
            state.selected_rows.push(PendingMetadataRecallRow {
                seed_index: matched.seed_index,
                row: row.clone(),
                row_match: matched.row_match,
            });
            Self::note_pending_recall_row(
                matched.seed_index,
                row,
                &mut *state.pending_contract_counts,
            );
            if state.selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                Self::drain_owned_recall_rows(
                    state.conn,
                    state.profiles,
                    &mut *state.selected_rows,
                    &mut *state.accumulators,
                    state.max_tokens_per_contract,
                )?;
                state.pending_contract_counts.clear();
            }
        }
        Ok(())
    }

    pub(super) fn score_name_recall_batch(
        profiles: &[SeedRecallProfile],
        batch_rows: &[RecallRow],
        name_threshold: f64,
    ) -> Vec<NameRecallMatch> {
        let mut matches = batch_rows
            .par_iter()
            .enumerate()
            .map(|(row_index, row)| {
                let mut row_matches = Vec::new();
                for (seed_index, profile) in profiles.iter().enumerate() {
                    if row.name_norm.is_empty()
                        || !profile.seed_name_queries.iter().any(|query| {
                            query
                                .score_percent(&row.name_norm, name_threshold)
                                .is_some()
                        })
                    {
                        continue;
                    }
                    row_matches.push(NameRecallMatch {
                        row_index,
                        seed_index,
                        row_match: SeedRowMatch {
                            token_uri_match: profile.exact_token_keys.contains(&row.token_uri_norm),
                            image_uri_match: profile.exact_image_keys.contains(&row.image_uri_norm),
                            name_prefix_match: true,
                            metadata_recall_match: false,
                        },
                    });
                }
                row_matches
            })
            .flatten()
            .collect::<Vec<_>>();
        matches.sort_by_key(|matched| (matched.row_index, matched.seed_index));
        matches
    }

    pub(super) fn append_metadata_recall_rows(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        metadata_threshold: f64,
        metadata_index: &MetadataRecallIndex,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if metadata_index.candidates.is_empty() {
            return Ok(());
        }
        let mut pending_metadata_rows = HashMap::new();
        let mut selected_rows = Vec::new();
        for (seed_index, profile) in profiles.iter().enumerate() {
            let Some(seed_doc) = profile.seed_metadata_doc.as_ref() else {
                continue;
            };
            let Some(seed_doc) = metadata_seed_doc_for_index(seed_doc, metadata_index) else {
                continue;
            };
            let seed_sketch =
                metadata_sketch_from_compact_corpus(&seed_doc, &metadata_index.compact_corpus);
            let seed_query =
                CompactMetadataBm25Query::new(&seed_doc, &metadata_index.compact_corpus);
            let candidate_indices = Self::metadata_source_candidate_indices(
                &seed_sketch,
                metadata_index,
                &profile.seed_contracts,
            );
            let mut matched_rows = candidate_indices
                .par_iter()
                .filter_map(|candidate_index| {
                    let candidate = metadata_index.candidates.get(*candidate_index)?;
                    let compact_document =
                        metadata_index.compact_documents.get(*candidate_index)?;
                    if !seed_query.has_term_overlap(compact_document)
                        || score_compact_metadata_indexed_pair_with_query(
                            &seed_query,
                            compact_document,
                        ) < metadata_threshold
                    {
                        return None;
                    }
                    let mut row = candidate.row.clone();
                    row.metadata_recall_match = true;
                    let row_match = Self::seed_row_match(profile, &row, 101.0, true);
                    Some((*candidate_index, row, row_match))
                })
                .collect::<Vec<_>>();
            matched_rows.sort_by_key(|(candidate_index, _, _)| *candidate_index);

            for (_, row, row_match) in matched_rows {
                if let Some(accumulator) = accumulators.get_mut(&profile.seed_address) {
                    if accumulator.mark_selected_metadata_recall(&row) {
                        continue;
                    }
                }
                let strong_match = row_match.token_uri_match
                    || row_match.image_uri_match
                    || row_match.name_prefix_match;
                if strong_match {
                    continue;
                }
                if max_tokens_per_contract > 0 {
                    if !Self::can_stage_metadata_recall_row(
                        profile,
                        &row,
                        accumulators,
                        max_tokens_per_contract,
                    ) {
                        continue;
                    }
                    Self::stage_metadata_recall_row(
                        &mut pending_metadata_rows,
                        seed_index,
                        &row,
                        row_match,
                        max_tokens_per_contract,
                    );
                    continue;
                }
                selected_rows.push(PendingMetadataRecallRow {
                    seed_index,
                    row,
                    row_match,
                });
                if selected_rows.len() >= SELECTED_RECALL_ROWID_CHUNK_SIZE {
                    Self::drain_owned_recall_rows(
                        conn,
                        profiles,
                        &mut selected_rows,
                        accumulators,
                        max_tokens_per_contract,
                    )?;
                }
            }
        }
        Self::drain_owned_recall_rows(
            conn,
            profiles,
            &mut selected_rows,
            accumulators,
            max_tokens_per_contract,
        )?;
        Self::drain_pending_metadata_recall_rows(
            conn,
            profiles,
            &mut pending_metadata_rows,
            accumulators,
            max_tokens_per_contract,
        )
    }

    pub(super) fn append_overlapping_metadata_token_rows(
        conn: &Connection,
        chain: &str,
        seed_token_ids: &HashSet<String>,
        rows_by_contract: &mut HashMap<String, ContractDuplicateRecord>,
    ) -> Result<(), AppError> {
        if seed_token_ids.is_empty() || rows_by_contract.is_empty() {
            return Ok(());
        }
        Self::prepare_seed_value_table(conn, SEED_TOKEN_ID_TABLE, seed_token_ids)?;
        let contract_key_by_lower = rows_by_contract
            .keys()
            .map(|key| (key.to_lowercase(), key.clone()))
            .collect::<BTreeMap<_, _>>();
        let contract_keys = contract_key_by_lower
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        Self::prepare_seed_value_table(conn, CANDIDATE_CONTRACT_TABLE, &contract_keys)?;

        let sql = format!(
            "
            SELECT contract_key, contract_address, token_id, token_uri, image_uri, name, symbol,
                   metadata_json
            FROM (
                SELECT lower(f.contract_address) AS contract_key,
                       f.contract_address, f.token_id, coalesce(f.token_uri, '') AS token_uri,
                       coalesce(f.image_uri, '') AS image_uri, coalesce(f.name, '') AS name,
                       coalesce(f.symbol, '') AS symbol, coalesce(f.metadata_json, '') AS metadata_json,
                       row_number() OVER (
                           PARTITION BY lower(f.contract_address)
                           ORDER BY CASE
                               WHEN trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '{{%'
                                    OR trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '[%' THEN 0
                               WHEN trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) <> '' THEN 1
                               ELSE 2
                           END,
                           CAST(f.token_id AS VARCHAR)
                       ) AS overlap_rank
                FROM nft_features f
                JOIN {CANDIDATE_CONTRACT_TABLE} c
                  ON c.value = lower(f.contract_address)
                WHERE f.chain = ?
                  AND CAST(f.token_id AS VARCHAR) IN (SELECT value FROM {SEED_TOKEN_ID_TABLE})
                  AND length(trim(coalesce(CAST(f.metadata_json AS VARCHAR), ''))) <= {MAX_METADATA_BYTES_FOR_DEDUP}
                  AND (
                      trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '{{%'
                      OR trim(coalesce(CAST(f.metadata_json AS VARCHAR), '')) LIKE '[%'
                  )
            )
            WHERE overlap_rank <= {MAX_OVERLAPPING_METADATA_ROWS_PER_CONTRACT}
            ORDER BY contract_key, token_id
            "
        );
        let mut stmt = conn.prepare(&sql)?;
        for batch in stmt.query_arrow(params![chain])? {
            let contract_key_column = arrow_string_column(&batch, 0, "contract_key")?;
            let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
            let token_column = arrow_string_column(&batch, 2, "token_id")?;
            let token_uri_column = arrow_string_column(&batch, 3, "token_uri")?;
            let image_uri_column = arrow_string_column(&batch, 4, "image_uri")?;
            let name_column = arrow_string_column(&batch, 5, "name")?;
            let symbol_column = arrow_string_column(&batch, 6, "symbol")?;
            let metadata_column = arrow_string_column(&batch, 7, "metadata_json")?;
            for row_index in 0..batch.num_rows() {
                let Some(original_key) =
                    contract_key_by_lower.get(contract_key_column.value(row_index))
                else {
                    continue;
                };
                let record = DatabaseNftRecord {
                    contract_address: contract_column.value(row_index).to_owned(),
                    token_id: token_column.value(row_index).to_owned(),
                    token_uri: token_uri_column.value(row_index).to_owned(),
                    image_uri: image_uri_column.value(row_index).to_owned(),
                    name: name_column.value(row_index).to_owned(),
                    symbol: symbol_column.value(row_index).to_owned(),
                    metadata_json: metadata_column.value(row_index).to_owned(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                };
                if let Some(entry) = rows_by_contract.get_mut(original_key) {
                    entry.metadata_token_rows.clear();
                    push_metadata_token_row(entry, &record);
                }
            }
        }
        Ok(())
    }

}
