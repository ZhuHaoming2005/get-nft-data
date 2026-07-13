use super::*;

impl DuckDbFeatureStore {
    fn estimate_metadata_recall_index_bytes(
        conn: &Connection,
        chain: &str,
    ) -> Result<usize, AppError> {
        let (row_count, contract_chars, document_chars): (i64, i64, i64) = conn.query_row(
            &format!(
                "SELECT CAST(count(*) AS BIGINT),
                        CAST(coalesce(sum(length(contract_address)), 0) AS BIGINT),
                        CAST(coalesce(sum(length(recall_doc)), 0) AS BIGINT)
                 FROM {METADATA_RECALL_DOC_TABLE} WHERE chain = ?"
            ),
            params![chain],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let row_count = usize::try_from(row_count.max(0)).unwrap_or(usize::MAX);
        let contract_chars = usize::try_from(contract_chars.max(0)).unwrap_or(usize::MAX);
        let document_chars = usize::try_from(document_chars.max(0)).unwrap_or(usize::MAX);
        Ok(row_count
            .saturating_mul(256)
            .saturating_add(contract_chars.saturating_mul(4))
            .saturating_add(document_chars.saturating_mul(12)))
    }

    pub(super) fn build_metadata_term_postings(
        compact_corpus: &crate::analysis::scoring::CompactMetadataBm25Corpus,
        compact_documents: &[crate::analysis::scoring::CompactMetadataBm25Document],
    ) -> Result<Vec<Vec<u32>>, AppError> {
        let mut postings = (0..compact_corpus.token_count())
            .map(|token_id| {
                Vec::with_capacity(compact_corpus.token_doc_freq_by_id(token_id as u32))
            })
            .collect::<Vec<_>>();
        for (candidate_index, document) in compact_documents.iter().enumerate() {
            let candidate_index = u32::try_from(candidate_index).map_err(|_| {
                AppError::InvalidData(
                    "metadata recall index exceeds the supported u32 candidate space".to_string(),
                )
            })?;
            for (token_id, _) in document.terms() {
                postings[*token_id as usize].push(candidate_index);
            }
        }
        Ok(postings)
    }

    pub(super) fn load_metadata_recall_index(
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
        memory_budget_bytes: usize,
    ) -> Result<MetadataRecallIndex, AppError> {
        if !prepared_recall_state.ready {
            return Err(AppError::InvalidData(format!(
                "prepared recall tables are required before loading metadata recall index for chain {chain:?}"
            )));
        }
        let sql = format!(
            "
            SELECT feature_rowid, contract_address, recall_doc
            FROM {METADATA_RECALL_DOC_TABLE}
            WHERE chain = ?
            "
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut candidates = Vec::new();
        let mut corpus_builder = CompactMetadataBm25CorpusBuilder::default();
        let mut compact_documents = Vec::new();
        let mut compact_document_bytes = 0usize;
        let mut posting_entry_count = 0usize;
        let mut candidate_bytes = 0usize;
        for batch in stmt.query_arrow(params![chain])? {
            let rowid_column = arrow_i64_column(&batch, 0, "feature_rowid")?;
            let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
            let recall_column = arrow_string_column(&batch, 2, "recall_doc")?;
            let batch_entries = (0..batch.num_rows())
                .into_par_iter()
                .filter_map(|row_index| {
                    let doc = MetadataBm25Document::from_text(recall_column.value(row_index))?;
                    Some((
                        doc,
                        rowid_column.value(row_index),
                        contract_column.value(row_index).to_owned(),
                    ))
                })
                .collect::<Vec<_>>();
            for (document, feature_rowid, contract_address) in batch_entries {
                let compact_document = corpus_builder.add_document(&document);
                posting_entry_count =
                    posting_entry_count.saturating_add(compact_document.terms().len());
                compact_document_bytes =
                    compact_document_bytes.saturating_add(compact_document.memory_bytes());
                compact_documents.push(compact_document);
                candidate_bytes = candidate_bytes
                    .saturating_add(std::mem::size_of::<MetadataRecallCandidate>())
                    .saturating_add(contract_address.capacity());
                candidates.push(MetadataRecallCandidate {
                    feature_rowid,
                    contract_address,
                });
            }
            let build_bytes = corpus_builder
                .memory_bytes()
                .saturating_add(compact_document_bytes)
                .saturating_add(candidate_bytes)
                .saturating_add(posting_entry_count.saturating_mul(std::mem::size_of::<u32>()))
                .saturating_add(
                    corpus_builder
                        .token_count()
                        .saturating_mul(std::mem::size_of::<Vec<u32>>()),
                );
            if build_bytes > memory_budget_bytes {
                return Err(AppError::ResourceLimit(format!(
                    "metadata recall index build for chain {chain:?} requires at least {build_bytes} bytes and exceeds its configured {memory_budget_bytes}-byte budget"
                )));
            }
        }
        let compact_corpus = corpus_builder.finish();
        let term_postings =
            Self::build_metadata_term_postings(&compact_corpus, &compact_documents)?;
        Ok(MetadataRecallIndex {
            candidates,
            compact_corpus,
            compact_documents,
            term_postings,
        })
    }

    pub(super) fn cached_metadata_recall_index(
        &self,
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<Arc<ManagedRecallIndex<MetadataRecallIndex>>, AppError> {
        if let Some(index) = self.metadata_recall_index_cache()?.get(chain) {
            return Ok(index);
        }
        let _build_guard = self
            .recall_index_build_lock
            .lock()
            .map_err(|err| AppError::DuckDb(format!("recall index build lock poisoned: {err}")))?;
        {
            let mut cache = self.metadata_recall_index_cache()?;
            if let Some(index) = cache.get(chain) {
                return Ok(index);
            }
        }

        let category = format!("metadata recall index build for chain {chain:?}");
        let estimated_bytes = Self::estimate_metadata_recall_index_bytes(conn, chain)?;
        let mut lease = self.reserve_recall_index_build(&category, estimated_bytes)?;
        let memory_budget_bytes = lease.bytes();
        let value = Self::load_metadata_recall_index(
            conn,
            chain,
            prepared_recall_state,
            memory_budget_bytes,
        )?;
        let index_bytes = value.memory_bytes();
        lease.resize(
            &format!("metadata recall index for chain {chain:?}"),
            index_bytes,
        )?;
        let index = Arc::new(ManagedRecallIndex {
            value,
            _lease: lease,
        });
        if !self.metadata_recall_index_cache()?.insert(
            chain.to_string(),
            Arc::clone(&index),
            index_bytes,
        ) {
            return Err(AppError::ResourceLimit(format!(
                "metadata recall index for chain {chain:?} requires approximately {index_bytes} bytes and exceeds its configured cache budget"
            )));
        }
        Ok(index)
    }

    pub(super) fn metadata_term_candidate_indices<'a>(
        seed_doc: &MetadataBm25Document,
        metadata_index: &MetadataRecallIndex,
        seed_contracts: &HashSet<String>,
        scratch: &'a mut MetadataCandidateScratch,
    ) -> &'a [u32] {
        scratch.clear();
        for token in seed_doc.tokens() {
            let Some(token_id) = metadata_index.compact_corpus.token_id(token) else {
                continue;
            };
            if let Some(postings) = metadata_index.term_postings.get(token_id as usize) {
                for candidate_index in postings {
                    let compact_candidate_index = *candidate_index;
                    let candidate_index = compact_candidate_index as usize;
                    if !scratch.insert(candidate_index) {
                        continue;
                    }
                    let Some(candidate) = metadata_index.candidates.get(candidate_index) else {
                        continue;
                    };
                    if !seed_contracts.contains(&candidate.contract_address) {
                        scratch.candidate_indices.push(compact_candidate_index);
                    }
                }
            }
        }
        scratch.candidate_indices.sort_unstable();
        &scratch.candidate_indices
    }
}
