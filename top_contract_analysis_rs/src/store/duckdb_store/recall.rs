use super::*;

impl DuckDbFeatureStore {
    pub(super) fn prepare_seed_uri_tables(
        conn: &Connection,
        profiles: &[SeedRecallProfile],
    ) -> Result<(), AppError> {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {SEED_TOKEN_URI_TABLE};
             DROP TABLE IF EXISTS {SEED_IMAGE_URI_TABLE};
             DROP TABLE IF EXISTS {SEED_CONTRACT_TABLE};
             DROP TABLE IF EXISTS {URI_SELECTED_MATCH_TABLE};
             CREATE TEMP TABLE {SEED_TOKEN_URI_TABLE} (
                 seed_index UINTEGER NOT NULL, value VARCHAR NOT NULL
             );
             CREATE TEMP TABLE {SEED_IMAGE_URI_TABLE} (
                 seed_index UINTEGER NOT NULL, value VARCHAR NOT NULL
             );
             CREATE TEMP TABLE {SEED_CONTRACT_TABLE} (
                 seed_index UINTEGER NOT NULL, contract_address VARCHAR NOT NULL
             );"
        ))?;
        let mut token_stmt =
            conn.prepare(&format!("INSERT INTO {SEED_TOKEN_URI_TABLE} VALUES (?, ?)"))?;
        let mut image_stmt =
            conn.prepare(&format!("INSERT INTO {SEED_IMAGE_URI_TABLE} VALUES (?, ?)"))?;
        let mut contract_stmt =
            conn.prepare(&format!("INSERT INTO {SEED_CONTRACT_TABLE} VALUES (?, ?)"))?;
        for (seed_index, profile) in profiles.iter().enumerate() {
            let seed_index = u32::try_from(seed_index).map_err(|_| {
                AppError::ResourceLimit(
                    "seed profile count exceeds the supported u32 candidate-staging space"
                        .to_string(),
                )
            })?;
            for value in profile.exact_token_keys.iter().collect::<BTreeSet<_>>() {
                if !value.is_empty() {
                    token_stmt.execute(params![seed_index, value])?;
                }
            }
            for value in profile.exact_image_keys.iter().collect::<BTreeSet<_>>() {
                if !value.is_empty() {
                    image_stmt.execute(params![seed_index, value])?;
                }
            }
            for contract in profile.seed_contracts.iter().collect::<BTreeSet<_>>() {
                contract_stmt.execute(params![seed_index, contract])?;
            }
        }
        Ok(())
    }

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
            DROP TABLE IF EXISTS {SEED_CONTRACT_TABLE};
            DROP TABLE IF EXISTS {CANDIDATE_CONTRACT_TABLE};
            DROP TABLE IF EXISTS {CANDIDATE_MATCH_TABLE};
            DROP TABLE IF EXISTS {URI_SELECTED_MATCH_TABLE};
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
            DROP TABLE IF EXISTS {CANDIDATE_MATCH_TABLE};
            CREATE TEMP TABLE {CANDIDATE_MATCH_TABLE} (
                seed_index UINTEGER NOT NULL,
                feature_rowid BIGINT NOT NULL,
                reason_bits UTINYINT NOT NULL
            );
            "
        ))?;
        Ok(())
    }

    pub(super) fn replace_selected_recall_rowids(
        conn: &Connection,
        rowids: &[i64],
    ) -> Result<(), AppError> {
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

    pub(super) fn fetch_recall_rows_by_feature_rowid(
        conn: &Connection,
        rowids: &[i64],
    ) -> Result<HashMap<i64, RecallRow>, AppError> {
        if rowids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut rows = HashMap::new();
        for chunk in rowids.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
            Self::replace_selected_recall_rowids(conn, chunk)?;
            let sql = format!(
                "
                SELECT f.rowid AS feature_rowid,
                       CASE WHEN lower(trim(CAST(f.chain AS VARCHAR))) = 'solana'
                            THEN trim(CAST(f.contract_address AS VARCHAR))
                            ELSE lower(trim(CAST(f.contract_address AS VARCHAR))) END AS contract_address,
                       CAST(f.token_id AS VARCHAR) AS token_id,
                       coalesce(CAST(f.token_uri_norm AS VARCHAR), '') AS token_uri_norm,
                       coalesce(CAST(f.image_uri_norm AS VARCHAR), '') AS image_uri_norm,
                       coalesce(CAST(f.name_norm AS VARCHAR), '') AS name_norm
                FROM nft_features f
                JOIN {SELECTED_RECALL_ROWID_TABLE} selected
                  ON selected.feature_rowid = f.rowid
                ORDER BY f.rowid
                "
            );
            let mut stmt = conn.prepare(&sql)?;
            for batch in stmt.query_arrow([])? {
                let mut batch_rows = Vec::new();
                append_recall_rows_from_arrow_batch(&batch, &mut batch_rows)?;
                rows.extend(batch_rows.into_iter().map(|row| (row.feature_rowid, row)));
            }
        }
        Ok(rows)
    }

    pub(super) fn append_exact_uri_recall_rows(
        &self,
        conn: &Connection,
        input: ExactUriRecallInput<'_>,
        _accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
    ) -> Result<(), AppError> {
        let ExactUriRecallInput {
            chain,
            profiles,
            all_token_keys,
            all_image_keys,
            prepared_recall_state,
        } = input;
        if !prepared_recall_state.ready {
            return Ok(());
        }
        if all_token_keys.is_empty() && all_image_keys.is_empty() {
            return Ok(());
        }

        Self::prepare_seed_uri_tables(conn, profiles)?;

        // Stage only fixed-width candidate identities. Full normalized URI
        // equality is checked here so hash collisions never become matches.
        // Cardinality admission and per-contract ranking happen before any
        // token/image/name/metadata payload is loaded into Rust.
        let stage_sql = format!(
            "
            INSERT INTO {CANDIDATE_MATCH_TABLE} (seed_index, feature_rowid, reason_bits)
            SELECT seed_index, feature_rowid, bit_or(reason_bits)::UTINYINT
            FROM (
                SELECT seed.seed_index, posting.feature_rowid, {TOKEN_URI_REASON}::UTINYINT AS reason_bits
                FROM {URI_RECALL_POSTING_TABLE} posting
                JOIN {SEED_TOKEN_URI_TABLE} seed
                  ON posting.uri_hash = md5_number_lower(seed.value)
                JOIN nft_features feature ON feature.rowid = posting.feature_rowid
                LEFT JOIN {SEED_CONTRACT_TABLE} own
                  ON own.seed_index = seed.seed_index
                 AND own.contract_address = feature.contract_address
                WHERE posting.chain = ? AND posting.uri_kind = 1
                  AND feature.token_uri_norm = seed.value
                  AND own.contract_address IS NULL
                UNION ALL
                SELECT seed.seed_index, posting.feature_rowid, {IMAGE_URI_REASON}::UTINYINT AS reason_bits
                FROM {URI_RECALL_POSTING_TABLE} posting
                JOIN {SEED_IMAGE_URI_TABLE} seed
                  ON posting.uri_hash = md5_number_lower(seed.value)
                JOIN nft_features feature ON feature.rowid = posting.feature_rowid
                LEFT JOIN {SEED_CONTRACT_TABLE} own
                  ON own.seed_index = seed.seed_index
                 AND own.contract_address = feature.contract_address
                WHERE posting.chain = ? AND posting.uri_kind = 2
                  AND feature.image_uri_norm = seed.value
                  AND own.contract_address IS NULL
            ) raw_matches
            GROUP BY seed_index, feature_rowid
            "
        );
        conn.execute(&stage_sql, params![chain, chain])?;

        let mut stats_stmt = conn.prepare(&format!(
            "SELECT candidate.seed_index,
                    CAST(count(DISTINCT feature.contract_address) AS UBIGINT),
                    CAST(count(*) AS UBIGINT)
             FROM {CANDIDATE_MATCH_TABLE} candidate
             JOIN nft_features feature ON feature.rowid = candidate.feature_rowid
             GROUP BY candidate.seed_index ORDER BY candidate.seed_index"
        ))?;
        let stats = stats_stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, u32>(0)? as usize,
                    row.get::<_, u64>(1)? as usize,
                    row.get::<_, u64>(2)? as usize,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stats_stmt);
        for (seed_index, contract_count, row_count) in stats {
            if contract_count > self.resource_options.max_candidate_contracts_per_seed {
                let seed = profiles
                    .get(seed_index)
                    .map(|profile| profile.seed_address.as_str())
                    .unwrap_or("<unknown>");
                return Err(AppError::ResourceLimit(format!(
                    "chain {chain:?}, seed {seed:?} URI recall matched {contract_count} candidate contracts and {row_count} rows, exceeding max_candidate_contracts_per_seed={} before payload loading",
                    self.resource_options.max_candidate_contracts_per_seed
                )));
            }
        }

        Ok(())
    }

    pub(super) fn append_staged_candidate_rows(
        conn: &Connection,
        rows: &[(usize, i64, u8)],
    ) -> Result<(), AppError> {
        for chunk in rows.chunks(SELECTED_RECALL_ROWID_VALUES_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let values = chunk
                .iter()
                .map(|(seed_index, feature_rowid, reason_bits)| {
                    format!("({seed_index}, {feature_rowid}, {reason_bits})")
                })
                .collect::<Vec<_>>()
                .join(",");
            conn.execute(
                &format!(
                    "INSERT INTO {CANDIDATE_MATCH_TABLE} (
                         seed_index, feature_rowid, reason_bits
                     ) VALUES {values}"
                ),
                [],
            )?;
        }
        Ok(())
    }

    pub(super) fn append_name_recall_rows(
        &self,
        conn: &Connection,
        input: NameRecallInput<'_>,
        _accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
    ) -> Result<(), AppError> {
        let NameRecallInput {
            chain,
            profiles,
            name_threshold,
            prepared_recall_state,
        } = input;
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
        let index = self.cached_name_recall_index(conn, chain, prepared_recall_state)?;
        let dense_scratch_bytes = index
            .rows
            .len()
            .saturating_mul(std::mem::size_of::<u16>())
            .saturating_mul(2);
        let use_dense_scratch = dense_scratch_bytes <= NAME_CANDIDATE_SCRATCH_BUDGET_BYTES;
        for (seed_index, profile) in profiles.iter().enumerate() {
            let matched_rows =
                Self::score_name_recall_profile(profile, &index, name_threshold, use_dense_scratch);
            // The exact index retains every distinct normalized name. Collapse
            // multiple matching names from one contract to a deterministic
            // feature row before applying contract guardrails and staging.
            let matched_contracts = matched_rows
                .into_iter()
                .filter_map(|row_index| index.rows.get(row_index))
                .fold(BTreeMap::<&str, i64>::new(), |mut rows, row| {
                    rows.entry(row.contract_address.as_str())
                        .and_modify(|feature_rowid| {
                            *feature_rowid = (*feature_rowid).min(row.feature_rowid);
                        })
                        .or_insert(row.feature_rowid);
                    rows
                });
            if matched_contracts.len() > self.resource_options.max_candidate_contracts_per_seed {
                return Err(AppError::ResourceLimit(format!(
                    "chain {chain:?}, seed {:?} name recall alone matched {} unique candidate contracts, exceeding max_candidate_contracts_per_seed={} before candidate staging",
                    profile.seed_address,
                    matched_contracts.len(),
                    self.resource_options.max_candidate_contracts_per_seed
                )));
            }
            let matched_feature_rowids = matched_contracts.into_values().collect::<Vec<_>>();
            for matched_chunk in matched_feature_rowids.chunks(SELECTED_RECALL_ROWID_CHUNK_SIZE) {
                let staged_rows = matched_chunk
                    .iter()
                    .map(|feature_rowid| (seed_index, *feature_rowid, NAME_REASON))
                    .collect::<Vec<_>>();
                Self::append_staged_candidate_rows(conn, &staged_rows)?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
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
        &self,
        conn: &Connection,
        profiles: &[SeedRecallProfile],
        metadata_threshold: f64,
        metadata_index: &MetadataRecallIndex,
        _accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
        _max_tokens_per_contract: usize,
    ) -> Result<(), AppError> {
        if metadata_index.candidates.is_empty() {
            return Ok(());
        }
        let mut matched_candidates = Vec::new();
        let mut metadata_candidate_scratch =
            MetadataCandidateScratch::new(metadata_index.candidates.len());
        for (seed_index, profile) in profiles.iter().enumerate() {
            let Some(seed_doc) = profile.seed_metadata_doc.as_ref() else {
                continue;
            };
            let seed_query =
                CompactMetadataBm25Query::new(seed_doc, &metadata_index.compact_corpus);
            let candidate_indices = Self::metadata_term_candidate_indices(
                seed_doc,
                metadata_index,
                &profile.seed_contracts,
                &mut metadata_candidate_scratch,
            );
            let mut metadata_match_count = 0usize;
            for candidate_chunk in candidate_indices.chunks(METADATA_MATCH_PAIR_CHUNK_SIZE) {
                let mut matched_indices = candidate_chunk
                    .par_iter()
                    .filter_map(|candidate_index| {
                        let candidate_index = *candidate_index as usize;
                        let compact_document =
                            metadata_index.compact_documents.get(candidate_index)?;
                        if !seed_query.has_term_overlap(compact_document)
                            || score_compact_metadata_indexed_pair_with_query(
                                &seed_query,
                                compact_document,
                            ) < metadata_threshold
                        {
                            return None;
                        }
                        Some(candidate_index)
                    })
                    .collect::<Vec<_>>();
                metadata_match_count = metadata_match_count.saturating_add(matched_indices.len());
                if metadata_match_count > self.resource_options.max_candidate_contracts_per_seed {
                    return Err(AppError::ResourceLimit(format!(
                        "seed {:?} metadata recall alone matched at least {metadata_match_count} unique candidate contracts, exceeding max_candidate_contracts_per_seed={} before completing candidate staging",
                        profile.seed_address,
                        self.resource_options.max_candidate_contracts_per_seed
                    )));
                }
                matched_indices.sort_unstable();
                for candidate_index in matched_indices {
                    matched_candidates.push((seed_index, candidate_index));
                    if matched_candidates.len() >= METADATA_MATCH_PAIR_CHUNK_SIZE {
                        Self::drain_metadata_candidate_matches(
                            conn,
                            metadata_index,
                            &mut matched_candidates,
                        )?;
                    }
                }
            }
        }
        Self::drain_metadata_candidate_matches(conn, metadata_index, &mut matched_candidates)?;
        Ok(())
    }

    pub(super) fn drain_metadata_candidate_matches(
        conn: &Connection,
        metadata_index: &MetadataRecallIndex,
        matched_candidates: &mut Vec<(usize, usize)>,
    ) -> Result<(), AppError> {
        if matched_candidates.is_empty() {
            return Ok(());
        }
        let staged_rows = matched_candidates
            .drain(..)
            .filter_map(|(seed_index, candidate_index)| {
                metadata_index
                    .candidates
                    .get(candidate_index)
                    .map(|candidate| (seed_index, candidate.feature_rowid, METADATA_REASON))
            })
            .collect::<Vec<_>>();
        Self::append_staged_candidate_rows(conn, &staged_rows)
    }

    pub(super) fn materialize_staged_recall_rows(
        &self,
        conn: &Connection,
        input: MaterializeRecallInput<'_>,
        accumulators: &mut BTreeMap<String, SnapshotAccumulator>,
    ) -> Result<(), AppError> {
        let MaterializeRecallInput {
            chain,
            profiles,
            name_threshold,
            max_tokens_per_contract,
            max_recall_rows,
        } = input;
        let aggregated = format!(
            "SELECT seed_index, feature_rowid, bit_or(reason_bits)::UTINYINT AS reason_bits
             FROM {CANDIDATE_MATCH_TABLE}
             GROUP BY seed_index, feature_rowid"
        );
        let mut stats_stmt = conn.prepare(&format!(
            "SELECT candidate.seed_index,
                    CAST(count(DISTINCT feature.contract_address) AS UBIGINT),
                    CAST(count(*) AS UBIGINT)
             FROM ({aggregated}) candidate
             JOIN nft_features feature ON feature.rowid = candidate.feature_rowid
             GROUP BY candidate.seed_index ORDER BY candidate.seed_index"
        ))?;
        for row in stats_stmt.query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)? as usize,
                row.get::<_, u64>(1)? as usize,
                row.get::<_, u64>(2)? as usize,
            ))
        })? {
            let (seed_index, contract_count, row_count) = row?;
            if contract_count > self.resource_options.max_candidate_contracts_per_seed {
                let seed = profiles
                    .get(seed_index)
                    .map(|profile| profile.seed_address.as_str())
                    .unwrap_or("<unknown>");
                return Err(AppError::ResourceLimit(format!(
                    "chain {chain:?}, seed {seed:?} recall matched {contract_count} candidate contracts and {row_count} rows, exceeding max_candidate_contracts_per_seed={} before payload loading",
                    self.resource_options.max_candidate_contracts_per_seed
                )));
            }
        }
        drop(stats_stmt);

        let token_cap_predicate = if max_tokens_per_contract == 0 {
            "TRUE".to_string()
        } else {
            format!("contract_rank <= {max_tokens_per_contract}")
        };
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {URI_SELECTED_MATCH_TABLE};
             CREATE TEMP TABLE {URI_SELECTED_MATCH_TABLE} AS
             SELECT seed_index, feature_rowid, reason_bits
             FROM (
                 SELECT candidate.seed_index, candidate.feature_rowid, candidate.reason_bits,
                        row_number() OVER (
                            PARTITION BY candidate.seed_index, feature.contract_address
                            ORDER BY
                                CASE WHEN (candidate.reason_bits & 3) <> 0 THEN 0 ELSE 1 END,
                                CASE WHEN (candidate.reason_bits & {NAME_REASON}) <> 0 THEN 0 ELSE 1 END,
                                CASE WHEN (candidate.reason_bits & {METADATA_REASON}) <> 0 THEN 0 ELSE 1 END,
                                CAST(feature.token_id AS VARCHAR)
                        ) AS contract_rank
                 FROM ({aggregated}) candidate
                 JOIN nft_features feature ON feature.rowid = candidate.feature_rowid
             ) ranked
             WHERE {token_cap_predicate}"
        ))?;
        let mut selected_stats = conn.prepare(&format!(
            "SELECT seed_index, CAST(count(*) AS UBIGINT)
             FROM {URI_SELECTED_MATCH_TABLE} GROUP BY seed_index ORDER BY seed_index"
        ))?;
        for row in selected_stats.query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)? as usize,
                row.get::<_, u64>(1)? as usize,
            ))
        })? {
            let (seed_index, selected_count) = row?;
            if selected_count > self.resource_options.max_selected_rows_per_seed {
                let seed = profiles
                    .get(seed_index)
                    .map(|profile| profile.seed_address.as_str())
                    .unwrap_or("<unknown>");
                return Err(AppError::ResourceLimit(format!(
                    "chain {chain:?}, seed {seed:?} selected {selected_count} rows after deterministic per-contract ranking, exceeding max_selected_rows_per_seed={} before payload loading",
                    self.resource_options.max_selected_rows_per_seed
                )));
            }
        }
        drop(selected_stats);

        // Reject oversized snapshots before Arrow or Rust owns any large
        // VARCHAR payload. DuckDB's encode + octet_length measures persisted
        // UTF-8 bytes exactly; the multipliers mirror the retained snapshot's
        // duplicate projections and normalized-key copies conservatively.
        let mut payload_stats = conn.prepare(&format!(
            "SELECT selected.seed_index,
                    CAST(count(*) AS UBIGINT),
                    CAST(coalesce(sum(
                        octet_length(encode(coalesce(CAST(feature.contract_address AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.token_id AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.token_uri AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.image_uri AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.name AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.symbol AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.metadata_json AS VARCHAR), '')))
                    ), 0) AS UBIGINT),
                    CAST(coalesce(sum(
                        octet_length(encode(coalesce(CAST(feature.contract_address AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.token_id AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.token_uri_norm AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.image_uri_norm AS VARCHAR), ''))) +
                        octet_length(encode(coalesce(CAST(feature.name_norm AS VARCHAR), '')))
                    ), 0) AS UBIGINT)
             FROM {URI_SELECTED_MATCH_TABLE} selected
             JOIN nft_features feature ON feature.rowid = selected.feature_rowid
             GROUP BY selected.seed_index ORDER BY selected.seed_index"
        ))?;
        for row in payload_stats.query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)? as usize,
                row.get::<_, u64>(1)? as usize,
                row.get::<_, u64>(2)? as usize,
                row.get::<_, u64>(3)? as usize,
            ))
        })? {
            let (seed_index, selected_count, payload_bytes, normalized_bytes) = row?;
            let estimated_bytes = payload_bytes
                .saturating_mul(4)
                .saturating_add(normalized_bytes.saturating_mul(3))
                .saturating_add(
                    selected_count.saturating_mul(SNAPSHOT_PREPAYLOAD_ROW_OVERHEAD_BYTES),
                );
            if estimated_bytes > self.resource_options.max_snapshot_bytes_per_seed {
                let seed = profiles
                    .get(seed_index)
                    .map(|profile| profile.seed_address.as_str())
                    .unwrap_or("<unknown>");
                return Err(AppError::ResourceLimit(format!(
                    "chain {chain:?}, seed {seed:?} selected payload is conservatively estimated at {estimated_bytes} bytes, exceeding max_snapshot_bytes_per_seed={} before payload loading",
                    self.resource_options.max_snapshot_bytes_per_seed
                )));
            }
        }
        drop(payload_stats);

        let mut stmt = conn.prepare(&format!(
            "SELECT selected.seed_index, selected.feature_rowid, selected.reason_bits
             FROM {URI_SELECTED_MATCH_TABLE} selected
             JOIN nft_features feature ON feature.rowid = selected.feature_rowid
             ORDER BY selected.seed_index, feature.contract_address,
                      CAST(feature.token_id AS VARCHAR)"
        ))?;
        for batch in stmt.query_arrow([])? {
            let seed_column = batch
                .column(0)
                .as_any()
                .downcast_ref::<duckdb::arrow::array::UInt32Array>()
                .ok_or_else(|| AppError::InvalidData("seed_index is not UINTEGER".to_string()))?;
            let rowid_column = arrow_i64_column(&batch, 1, "feature_rowid")?;
            let reason_column = batch
                .column(2)
                .as_any()
                .downcast_ref::<duckdb::arrow::array::UInt8Array>()
                .ok_or_else(|| AppError::InvalidData("reason_bits is not UTINYINT".to_string()))?;
            let selected = (0..batch.num_rows())
                .map(|row_index| {
                    (
                        seed_column.value(row_index) as usize,
                        rowid_column.value(row_index),
                        reason_column.value(row_index),
                    )
                })
                .collect::<Vec<_>>();
            let recall_batch_size = if max_recall_rows == 0 {
                DEFAULT_RECALL_BATCH_SIZE
            } else {
                max_recall_rows
            };
            for selected_chunk in selected.chunks(recall_batch_size) {
                let rowids = selected_chunk
                    .iter()
                    .map(|(_, feature_rowid, _)| *feature_rowid)
                    .collect::<Vec<_>>();
                let recall_rows = Self::fetch_recall_rows_by_feature_rowid(conn, &rowids)?;
                let records = Self::fetch_records_by_feature_rowid(conn, &rowids)?;
                for &(seed_index, feature_rowid, reason_bits) in selected_chunk {
                    let Some(profile) = profiles.get(seed_index) else {
                        continue;
                    };
                    let Some(row) = recall_rows.get(&feature_rowid) else {
                        continue;
                    };
                    let Some(record) = records.get(&feature_rowid) else {
                        continue;
                    };
                    let row_match = Self::seed_row_match(
                        profile,
                        row,
                        name_threshold,
                        reason_bits & METADATA_REASON != 0,
                    );
                    if let Some(accumulator) = accumulators.get_mut(&profile.seed_address) {
                        accumulator.push_recall_row(
                            profile,
                            row,
                            record.clone(),
                            &row_match,
                            max_tokens_per_contract,
                        );
                    }
                }
            }
        }
        Ok(())
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
        let preserve_case = chain.trim().eq_ignore_ascii_case("solana");
        let contract_key_by_normalized = rows_by_contract
            .keys()
            .map(|key| {
                (
                    if preserve_case {
                        key.trim().to_string()
                    } else {
                        key.trim().to_lowercase()
                    },
                    key.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let contract_keys = contract_key_by_normalized
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        Self::prepare_seed_value_table(conn, CANDIDATE_CONTRACT_TABLE, &contract_keys)?;

        let contract_key_sql = if preserve_case {
            "trim(f.contract_address)"
        } else {
            "lower(trim(f.contract_address))"
        };
        let sql = format!(
            "
            SELECT contract_key, contract_address, token_id, token_uri, image_uri, name, symbol,
                   metadata_json
            FROM (
                SELECT {contract_key_sql} AS contract_key,
                       f.contract_address, f.token_id, coalesce(f.token_uri, '') AS token_uri,
                       coalesce(f.image_uri, '') AS image_uri, coalesce(f.name, '') AS name,
                       coalesce(f.symbol, '') AS symbol, coalesce(f.metadata_json, '') AS metadata_json,
                       row_number() OVER (
                           PARTITION BY {contract_key_sql}
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
                  ON c.value = {contract_key_sql}
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
                    contract_key_by_normalized.get(contract_key_column.value(row_index))
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
