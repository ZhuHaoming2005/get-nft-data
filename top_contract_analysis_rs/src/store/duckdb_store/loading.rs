use super::*;
use sha2::{Digest, Sha256};
use std::io::{BufReader, Read};

const AUTHORITATIVE_INPUT_HASH_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const AUTHORITATIVE_INPUT_HASH_MAX_CONCURRENCY: usize = 8;

struct StableFileDigest {
    size_bytes: u64,
    modified_unix_nanos: u128,
    sha256: [u8; 32],
}

impl DuckDbFeatureStore {
    pub(super) fn unix_time_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    pub(super) fn fingerprint_authoritative_inputs(
        parquet_paths: &[String],
    ) -> Result<AuthoritativeInputFingerprint, AppError> {
        if parquet_paths.is_empty() {
            return Err(AppError::InvalidData(
                "authoritative import requires at least one Parquet input".to_string(),
            ));
        }
        let mut canonical_inputs = parquet_paths
            .iter()
            .map(|path| {
                std::fs::canonicalize(path)
                    .map(|canonical| canonical.to_string_lossy().into_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        canonical_inputs.sort();
        canonical_inputs.dedup();

        let hashed_inputs =
            if let Some(pool) = Self::authoritative_input_hash_pool(canonical_inputs.len())? {
                pool.install(|| {
                    canonical_inputs
                        .par_iter()
                        .map(|path| Self::stable_file_digest(Path::new(path)))
                        .collect::<Result<Vec<_>, _>>()
                })?
            } else {
                canonical_inputs
                    .iter()
                    .map(|path| Self::stable_file_digest(Path::new(path)))
                    .collect::<Result<Vec<_>, _>>()?
            };

        let mut combined = Sha256::new();
        let mut manifest = Vec::with_capacity(canonical_inputs.len());
        for (path, hashed) in canonical_inputs.iter().zip(hashed_inputs) {
            let file_sha256 = Self::hex_digest(&hashed.sha256);
            combined.update((path.len() as u64).to_le_bytes());
            combined.update(path.as_bytes());
            combined.update(hashed.size_bytes.to_le_bytes());
            combined.update(hashed.sha256);
            manifest.push(AuthoritativeInputManifestEntry {
                path: path.clone(),
                size_bytes: hashed.size_bytes,
                modified_unix_nanos: hashed.modified_unix_nanos,
                sha256: file_sha256,
            });
        }
        let combined_sha256 = Self::hex_digest(&combined.finalize());
        Ok(AuthoritativeInputFingerprint {
            combined_sha256,
            canonical_inputs,
            manifest,
        })
    }

    pub(super) fn validate_authoritative_inputs_unchanged(
        fingerprint: &AuthoritativeInputFingerprint,
    ) -> Result<(), AppError> {
        if let Some(pool) = Self::authoritative_input_hash_pool(fingerprint.manifest.len())? {
            pool.install(|| {
                fingerprint
                    .manifest
                    .par_iter()
                    .try_for_each(Self::validate_authoritative_input_unchanged)
            })
        } else {
            fingerprint
                .manifest
                .iter()
                .try_for_each(Self::validate_authoritative_input_unchanged)
        }
    }

    fn authoritative_input_hash_pool(
        job_count: usize,
    ) -> Result<Option<rayon::ThreadPool>, AppError> {
        let thread_count = job_count.min(AUTHORITATIVE_INPUT_HASH_MAX_CONCURRENCY);
        if thread_count <= 1 {
            return Ok(None);
        }
        rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .thread_name(|index| format!("authoritative-input-hash-{index}"))
            .build()
            .map(Some)
            .map_err(|error| {
                AppError::InvalidData(format!(
                    "failed to create authoritative input hash workers: {error}"
                ))
            })
    }

    fn stable_file_digest(path: &Path) -> Result<StableFileDigest, AppError> {
        let before = std::fs::metadata(path)?;
        let before_modified = before.modified()?;
        let sha256 = Self::sha256_file(path)?;
        let after = std::fs::metadata(path)?;
        let after_modified = after.modified()?;
        if before.len() != after.len() || before_modified != after_modified {
            return Err(AppError::InvalidData(format!(
                "Parquet input {:?} changed while it was being fingerprinted",
                path
            )));
        }
        Ok(StableFileDigest {
            size_bytes: after.len(),
            modified_unix_nanos: after_modified
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            sha256,
        })
    }

    fn validate_authoritative_input_unchanged(
        expected: &AuthoritativeInputManifestEntry,
    ) -> Result<(), AppError> {
        let before = std::fs::metadata(&expected.path)?;
        let before_modified = before.modified()?;
        let before_modified_unix_nanos = before_modified
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        if before.len() != expected.size_bytes
            || before_modified_unix_nanos != expected.modified_unix_nanos
        {
            return Err(AppError::InvalidData(format!(
                "authoritative Parquet input {:?} changed after its initial fingerprint",
                expected.path
            )));
        }

        let file_digest = Self::sha256_file(Path::new(&expected.path))?;
        let after = std::fs::metadata(&expected.path)?;
        let after_modified = after.modified()?;
        let after_modified_unix_nanos = after_modified
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let actual_sha256 = Self::hex_digest(&file_digest);
        if before.len() != after.len()
            || before_modified != after_modified
            || after.len() != expected.size_bytes
            || after_modified_unix_nanos != expected.modified_unix_nanos
            || actual_sha256 != expected.sha256
        {
            return Err(AppError::InvalidData(format!(
                "authoritative Parquet input {:?} changed while the import transaction was running",
                expected.path
            )));
        }
        Ok(())
    }

    fn sha256_file(path: &Path) -> Result<[u8; 32], AppError> {
        let mut reader = BufReader::with_capacity(
            AUTHORITATIVE_INPUT_HASH_BUFFER_BYTES,
            std::fs::File::open(path)?,
        );
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; AUTHORITATIVE_INPUT_HASH_BUFFER_BYTES];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hasher.finalize().into())
    }

    fn hex_digest(digest: &[u8]) -> String {
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    pub(super) fn prepare_journal_state_from_connection(
        conn: &Connection,
    ) -> Result<Option<PrepareJournalState>, AppError> {
        if !Self::table_exists(conn, PREPARE_JOURNAL_TABLE)? {
            return Ok(None);
        }
        let mut stmt = conn.prepare(&format!(
            "SELECT input_fingerprint, canonical_inputs_json, expected_chains_json,
                    imported_generations_json, prepared_chains_json, phase,
                    prepared_format_version, normalization_version,
                    recall_algorithm_version, report_schema_version, build_fingerprint
             FROM {PREPARE_JOURNAL_TABLE} WHERE journal_id = 1 LIMIT 1"
        ))?;
        let mut rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
            ))
        })?;
        let Some(row) = rows.next() else {
            return Ok(None);
        };
        let (
            input_fingerprint,
            _inputs_json,
            chains_json,
            generations_json,
            prepared_json,
            phase,
            prepared_format_version,
            normalization_version,
            recall_algorithm_version,
            report_schema_version,
            build_fingerprint,
        ) = row?;
        Ok(Some(PrepareJournalState {
            input_fingerprint,
            expected_chains: serde_json::from_str(&chains_json)?,
            imported_generations: serde_json::from_str(&generations_json)?,
            prepared_chains: serde_json::from_str(&prepared_json)?,
            phase,
            prepared_format_version,
            normalization_version,
            recall_algorithm_version,
            report_schema_version,
            build_fingerprint,
        }))
    }

    pub(super) fn record_prepare_journal_error(&self, error: &AppError) {
        let message = error.to_string().chars().take(16_384).collect::<String>();
        let result = (|| -> Result<(), AppError> {
            let conn = self.conn()?;
            if !Self::table_exists(&conn, PREPARE_JOURNAL_TABLE)? {
                return Ok(());
            }
            conn.execute(
                &format!(
                    "UPDATE {PREPARE_JOURNAL_TABLE}
                     SET updated_at_ms = ?, last_error = ?
                     WHERE journal_id = 1"
                ),
                params![Self::unix_time_millis(), message],
            )?;
            Ok(())
        })();
        if let Err(journal_error) = result {
            eprintln!(
                "warning: failed to persist prepare journal error after {error}: {journal_error}"
            );
        }
    }

    fn write_fingerprinted_prepare_journal(
        conn: &Connection,
        fingerprint: &AuthoritativeInputFingerprint,
    ) -> Result<(), AppError> {
        let now = Self::unix_time_millis();
        let run_id = Self::new_feature_generation_id();
        let inputs_json = serde_json::to_string(&fingerprint.canonical_inputs)?;
        let manifest_json = serde_json::to_string(&fingerprint.manifest)?;
        let transaction = conn.unchecked_transaction()?;
        transaction.execute(
            &format!("DELETE FROM {PREPARE_JOURNAL_TABLE} WHERE journal_id = 1"),
            [],
        )?;
        transaction.execute(
            &format!(
                "INSERT INTO {PREPARE_JOURNAL_TABLE} (
                    journal_id, run_id, input_fingerprint, canonical_inputs_json,
                    input_manifest_json,
                    expected_chains_json, imported_generations_json,
                    prepared_chains_json, phase, prepared_format_version,
                    normalization_version, recall_algorithm_version,
                    report_schema_version, build_fingerprint,
                    started_at_ms, updated_at_ms, last_error
                 ) VALUES (1, ?, ?, ?, ?, '[]', '{{}}', '[]', 'fingerprinted',
                           ?, ?, ?, ?, ?, ?, ?, '')"
            ),
            params![
                run_id,
                fingerprint.combined_sha256,
                inputs_json,
                manifest_json,
                PREPARED_FORMAT_VERSION,
                NORMALIZATION_VERSION,
                RECALL_ALGORITHM_VERSION,
                REPORT_SCHEMA_VERSION,
                BUILD_FINGERPRINT,
                now,
                now
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn validate_journal_generations(
        conn: &Connection,
        journal: &PrepareJournalState,
    ) -> Result<(), AppError> {
        if journal.prepared_format_version != PREPARED_FORMAT_VERSION
            || journal.normalization_version != NORMALIZATION_VERSION
            || journal.recall_algorithm_version != RECALL_ALGORITHM_VERSION
            || journal.report_schema_version != REPORT_SCHEMA_VERSION
            || journal.build_fingerprint != BUILD_FINGERPRINT
        {
            return Err(AppError::InvalidData(
                "prepare journal was created by an incompatible format, algorithm, or build; rerun with --restart-prepare"
                    .to_string(),
            ));
        }
        if journal.expected_chains.is_empty() || journal.imported_generations.is_empty() {
            return Err(AppError::InvalidData(
                "prepare journal has no committed authoritative import; provide Parquet inputs or use --restart-prepare"
                    .to_string(),
            ));
        }
        for chain in &journal.expected_chains {
            let current = Self::feature_generation_state(conn, chain)?.ok_or_else(|| {
                AppError::InvalidData(format!(
                    "committed feature generation is missing for chain {chain:?}; rerun with --restart-prepare"
                ))
            })?;
            let expected = journal.imported_generations.get(chain).ok_or_else(|| {
                AppError::InvalidData(format!(
                    "prepare journal is missing the imported generation for chain {chain:?}"
                ))
            })?;
            if &current.generation_id != expected {
                return Err(AppError::InvalidData(format!(
                    "feature generation changed after the committed import for chain {chain:?}; rerun with --restart-prepare"
                )));
            }
        }
        Ok(())
    }

    fn journal_chains(
        journal: &PrepareJournalState,
    ) -> Result<Vec<crate::models::Chain>, AppError> {
        journal
            .expected_chains
            .iter()
            .map(|chain| chain.parse::<crate::models::Chain>())
            .collect()
    }

    pub fn import_authoritative_parquet_snapshot(
        &self,
        parquet_paths: &[String],
        restart_prepare: bool,
    ) -> Result<Vec<crate::models::Chain>, AppError> {
        let result = (|| -> Result<Vec<crate::models::Chain>, AppError> {
            if !self.writable {
                return Err(AppError::InvalidData(
                    "authoritative import requires a writable database".to_string(),
                ));
            }
            let fingerprint = Self::fingerprint_authoritative_inputs(parquet_paths)?;
            {
                let conn = self.conn()?;
                if let Some(journal) = Self::prepare_journal_state_from_connection(&conn)? {
                    if !restart_prepare && journal.input_fingerprint == fingerprint.combined_sha256
                    {
                        if journal.phase != "fingerprinted" {
                            Self::validate_journal_generations(&conn, &journal)?;
                            return Self::journal_chains(&journal);
                        }
                    } else if !restart_prepare && journal.phase != "ready" {
                        return Err(AppError::InvalidData(format!(
                        "an unfinished prepare run for a different input fingerprint is at phase {:?}; rerun with the original inputs or use --restart-prepare",
                        journal.phase
                    )));
                    }
                }
                Self::write_fingerprinted_prepare_journal(&conn, &fingerprint)?;
            }
            let chains = self.load_parquet_datasets_auto_inner(
                &fingerprint.canonical_inputs,
                Some(&fingerprint),
            )?;
            self.conn()?.execute_batch("CHECKPOINT")?;
            Ok(chains)
        })();
        if let Err(error) = &result {
            self.record_prepare_journal_error(error);
        }
        result
    }

    pub fn resume_authoritative_prepare(&self) -> Result<Vec<crate::models::Chain>, AppError> {
        let conn = self.conn()?;
        let journal = Self::prepare_journal_state_from_connection(&conn)?.ok_or_else(|| {
            AppError::InvalidData(
                "prepare journal is missing; provide authoritative Parquet inputs first"
                    .to_string(),
            )
        })?;
        if journal.phase == "fingerprinted" {
            return Err(AppError::InvalidData(
                "the authoritative import was not committed; rerun with the original Parquet inputs"
                    .to_string(),
            ));
        }
        Self::validate_journal_generations(&conn, &journal)?;
        Self::journal_chains(&journal)
    }

    #[cfg(test)]
    pub(super) fn prepare_journal_phase(&self) -> Result<Option<String>, AppError> {
        let conn = self.conn()?;
        Ok(Self::prepare_journal_state_from_connection(&conn)?.map(|state| state.phase))
    }

    pub(super) fn new_feature_generation_id() -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let sequence = FEATURE_GENERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        format!(
            "{timestamp:032x}-{:08x}-{sequence:016x}",
            std::process::id()
        )
    }

    pub(super) fn feature_generation_state(
        conn: &Connection,
        chain: &str,
    ) -> Result<Option<FeatureGenerationState>, AppError> {
        if !Self::table_exists(conn, FEATURE_GENERATION_TABLE)? {
            return Ok(None);
        }
        let mut stmt = conn.prepare(&format!(
            "SELECT generation_id, feature_row_count, contract_count
             FROM {FEATURE_GENERATION_TABLE}
             WHERE chain = ? LIMIT 1"
        ))?;
        let mut rows = stmt.query_map(params![chain], |row| {
            Ok(FeatureGenerationState {
                generation_id: row.get(0)?,
                row_count: row.get(1)?,
                contract_count: row.get(2)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub(super) fn record_feature_generation(
        conn: &Connection,
        chain: &str,
    ) -> Result<FeatureGenerationState, AppError> {
        let (row_count, contract_count) = conn.query_row(
            "SELECT CAST(count(*) AS BIGINT),
                    CAST(count(DISTINCT contract_address) AS BIGINT)
             FROM nft_features WHERE chain = ?",
            params![chain],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        Self::record_feature_generation_with_counts(conn, chain, row_count, contract_count)
    }

    pub(super) fn record_feature_generation_with_counts(
        conn: &Connection,
        chain: &str,
        row_count: i64,
        contract_count: i64,
    ) -> Result<FeatureGenerationState, AppError> {
        let state = FeatureGenerationState {
            generation_id: Self::new_feature_generation_id(),
            row_count,
            contract_count,
        };
        conn.execute(
            &format!("DELETE FROM {FEATURE_GENERATION_TABLE} WHERE chain = ?"),
            params![chain],
        )?;
        conn.execute(
            &format!(
                "INSERT INTO {FEATURE_GENERATION_TABLE} (
                    chain, generation_id, feature_row_count, contract_count
                 ) VALUES (?, ?, ?, ?)"
            ),
            params![
                chain,
                state.generation_id,
                state.row_count,
                state.contract_count
            ],
        )?;
        Ok(state)
    }

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
        if let Some(generation) = Self::feature_generation_state(&conn, chain)? {
            return Ok(generation.row_count > 0);
        }
        let exists = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM nft_features WHERE chain = ? LIMIT 1)",
            params![chain],
            |row| row.get::<_, bool>(0),
        )?;
        Ok(exists)
    }

    pub fn chain_totals(&self, chain: &str) -> Result<crate::models::ChainTotalsPayload, AppError> {
        let conn = self.conn()?;
        if let Some(generation) = Self::feature_generation_state(&conn, chain)? {
            return Ok(crate::models::ChainTotalsPayload {
                total_nfts: generation.row_count,
                total_contracts: generation.contract_count,
            });
        }
        // Compatibility fallback for tests and legacy callers that populated
        // nft_features directly. Production prepare-features always records a
        // generation, so analyze/batch remain O(1) here.
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
        let generation = Self::feature_generation_state(&conn, chain)?.ok_or_else(|| {
            AppError::InvalidData(format!(
                "feature generation identity is missing for chain {chain:?}; run prepare-features first"
            ))
        })?;
        let identity = format!(
            "{}:{}:{}:{}:{}:{}:{}:{}",
            generation.generation_id,
            generation.row_count,
            generation.contract_count,
            PREPARED_FORMAT_VERSION,
            NORMALIZATION_VERSION,
            RECALL_ALGORITHM_VERSION,
            REPORT_SCHEMA_VERSION,
            BUILD_FINGERPRINT
        );
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
        Self::record_feature_generation(&transaction, chain)?;
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
                        CAST(name AS VARCHAR), CAST(symbol AS VARCHAR),
                        CAST(metadata_json AS VARCHAR),
                        CAST(token_uri_norm AS VARCHAR),
                        CAST(image_uri_norm AS VARCHAR),
                        CAST(name_norm AS VARCHAR)
                ) AS source_rank
                FROM read_parquet({path})
                WHERE lower(trim(CAST(chain AS VARCHAR))) = ?
            ) source
            WHERE source_rank = 1
            ORDER BY contract_address, token_id
            ",
        );
        let mut conn = self.conn()?;
        let transaction = conn.transaction()?;
        transaction.execute("DELETE FROM nft_features WHERE chain = ?", params![chain])?;
        transaction.execute(&insert_sql, params![chain, chain])?;
        Self::record_feature_generation(&transaction, chain)?;
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
        self.load_parquet_datasets_auto_inner(parquet_paths, None)
    }

    fn load_parquet_datasets_auto_inner(
        &self,
        parquet_paths: &[String],
        journal_fingerprint: Option<&AuthoritativeInputFingerprint>,
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
            "CREATE OR REPLACE TEMP VIEW incoming_nft_features AS
             SELECT * FROM read_parquet([{path_list}], union_by_name = true)"
        ))?;
        let columns = Self::table_columns(&conn, "incoming_nft_features")?;
        let missing = REQUIRED_SNAPSHOT_COLUMNS
            .iter()
            .copied()
            .filter(|column| !columns.contains(*column))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            conn.execute_batch("DROP VIEW incoming_nft_features")?;
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
            conn.execute_batch("DROP VIEW incoming_nft_features")?;
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
                conn.execute_batch("DROP VIEW incoming_nft_features")?;
                return Err(error);
            }
        };
        drop(conn);
        let mut imported_generations = BTreeMap::new();
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
                              CAST(name AS VARCHAR), CAST(symbol AS VARCHAR),
                              CAST(metadata_json AS VARCHAR),
                              CAST(token_uri_norm AS VARCHAR),
                              CAST(image_uri_norm AS VARCHAR),
                              CAST(name_norm AS VARCHAR)
                      ) AS source_rank
                  FROM incoming_nft_features
              ) source
              WHERE source.source_rank = 1;

             DELETE FROM nft_features
             WHERE chain IN (
                 SELECT DISTINCT chain_norm FROM deduped_incoming_nft_features
             );

             INSERT INTO nft_features (
                 chain, contract_address, token_id, token_uri, image_uri, name, symbol,
                 metadata_json, token_uri_norm, image_uri_norm, name_norm
             )
             SELECT chain_norm, contract_address_norm, token_id_norm, token_uri_value,
                    image_uri_value, name_value, symbol_value, metadata_json_value,
                    token_uri_norm_value, image_uri_norm_value, name_norm_value
             FROM deduped_incoming_nft_features
             ORDER BY chain_norm, contract_address_norm, token_id_norm",
        )?;
        let generation_counts = {
            let mut stmt = transaction.prepare(
                "SELECT chain,
                        CAST(count(*) AS BIGINT) AS row_count,
                        CAST(count(DISTINCT contract_address) AS BIGINT) AS contract_count
                 FROM nft_features
                 WHERE chain IN (
                     SELECT DISTINCT chain_norm FROM deduped_incoming_nft_features
                 )
                 GROUP BY chain",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            let mut counts = HashMap::new();
            for row in rows {
                let (chain, row_count, contract_count) = row?;
                counts.insert(chain, (row_count, contract_count));
            }
            counts
        };
        for chain in &chains {
            let (row_count, contract_count) = generation_counts
                .get(chain.as_str())
                .copied()
                .ok_or_else(|| {
                    AppError::InvalidData(format!(
                        "imported feature generation counts are missing chain {:?}",
                        chain.as_str()
                    ))
                })?;
            let generation = Self::record_feature_generation_with_counts(
                &transaction,
                chain.as_str(),
                row_count,
                contract_count,
            )?;
            imported_generations.insert(chain.as_str().to_string(), generation.generation_id);
            transaction.execute(
                &format!("DELETE FROM {PREPARED_RECALL_CHAIN_TABLE} WHERE chain = ?"),
                params![chain.as_str()],
            )?;
        }
        if let Some(fingerprint) = journal_fingerprint {
            let expected_chains = chains
                .iter()
                .map(|chain| chain.as_str())
                .collect::<Vec<_>>();
            let changed = transaction.execute(
                &format!(
                    "UPDATE {PREPARE_JOURNAL_TABLE}
                     SET expected_chains_json = ?, imported_generations_json = ?,
                         prepared_chains_json = '[]', phase = 'imported',
                         updated_at_ms = ?, last_error = ''
                     WHERE journal_id = 1 AND input_fingerprint = ?"
                ),
                params![
                    serde_json::to_string(&expected_chains)?,
                    serde_json::to_string(&imported_generations)?,
                    Self::unix_time_millis(),
                    fingerprint.combined_sha256
                ],
            )?;
            if changed != 1 {
                return Err(AppError::InvalidData(
                    "prepare journal changed before authoritative import commit".to_string(),
                ));
            }
        }
        transaction.execute_batch(
            "DROP TABLE deduped_incoming_nft_features;
             DROP VIEW incoming_nft_features",
        )?;
        if let Some(fingerprint) = journal_fingerprint {
            // The files are read by DuckDB after their initial SHA-256 pass.
            // Rehash before commit so a replacement or concurrent writer rolls
            // back the authoritative feature generation atomically.
            Self::validate_authoritative_inputs_unchanged(fingerprint)?;
        }
        transaction.commit()?;
        Ok(chains)
    }

    pub(super) fn sql_string_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    pub(super) fn sql_metadata_json_eligible_predicate(column: &str) -> String {
        // Keep in sync with dedup metadata JSON eligibility.
        // and metadata_is_dedup_eligible: trim, non-empty, len<=64KiB, starts with { or [
        let trimmed = format!("trim(coalesce(CAST({column} AS VARCHAR), ''))");
        format!(
            "length({trimmed}) <= {MAX_METADATA_BYTES_FOR_DEDUP} \
             AND ({trimmed} LIKE '{{%' \
             OR {trimmed} LIKE '[%')"
        )
    }
}
