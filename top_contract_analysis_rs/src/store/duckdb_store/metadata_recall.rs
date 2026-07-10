use super::*;

impl DuckDbFeatureStore {
    pub(super) fn build_metadata_source_index(candidates: &[MetadataRecallCandidate]) -> MetadataSourceIndex {
        let new_index = || MetadataSourceIndex {
            anchor_indices: HashMap::new(),
            simhash_band_indices: vec![
                Vec::new();
                METADATA_SIMHASH_BAND_COUNT * METADATA_SIMHASH_BAND_VALUES
            ],
        };
        let mut index = candidates
            .par_iter()
            .enumerate()
            .fold(new_index, |mut local, (candidate_index, candidate)| {
                if candidate.sketch.simhash == 0 && candidate.sketch.anchors.is_empty() {
                    return local;
                }
                for anchor in &candidate.sketch.anchors {
                    local
                        .anchor_indices
                        .entry(anchor.clone())
                        .or_default()
                        .push(candidate_index);
                }
                for band_index in 0..METADATA_SIMHASH_BAND_COUNT {
                    let band_value =
                        metadata_simhash_band_value(candidate.sketch.simhash, band_index);
                    local.simhash_band_indices[metadata_simhash_band_key(band_index, band_value)]
                        .push(candidate_index);
                }
                local
            })
            .reduce(new_index, |mut left, mut right| {
                for (anchor, mut indices) in right.anchor_indices {
                    left.anchor_indices
                        .entry(anchor)
                        .or_default()
                        .append(&mut indices);
                }
                for (left_bucket, right_bucket) in left
                    .simhash_band_indices
                    .iter_mut()
                    .zip(right.simhash_band_indices.iter_mut())
                {
                    left_bucket.append(right_bucket);
                }
                left
            });
        index
            .anchor_indices
            .par_iter_mut()
            .for_each(|(_, indices)| indices.sort_unstable());
        index
            .simhash_band_indices
            .par_iter_mut()
            .for_each(|indices| indices.sort_unstable());
        index
    }

    pub(super) fn load_metadata_recall_index(
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<MetadataRecallIndex, AppError> {
        if !prepared_recall_state.ready {
            return Err(AppError::InvalidData(format!(
                "prepared recall tables are required before loading metadata recall index for chain {chain:?}"
            )));
        }
        let sql = format!(
            "
            SELECT feature_rowid, contract_address, token_id, token_uri_norm, image_uri_norm,
                   name_norm, recall_doc
            FROM {METADATA_RECALL_DOC_TABLE}
            WHERE chain = ?
            ORDER BY feature_rowid
            "
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut candidates = Vec::new();
        // `docs` holds the string-keyed BM25 documents only for as long as the
        // corpus, compact documents and sketches need them; it is dropped before
        // the index is returned so the per-candidate token vectors and
        // term-frequency hashmaps do not stay resident alongside the compact
        // representation.
        let mut docs = Vec::new();
        for batch in stmt.query_arrow(params![chain])? {
            let rowid_column = arrow_i64_column(&batch, 0, "feature_rowid")?;
            let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
            let token_column = arrow_string_column(&batch, 2, "token_id")?;
            let token_uri_column = arrow_string_column(&batch, 3, "token_uri_norm")?;
            let image_uri_column = arrow_string_column(&batch, 4, "image_uri_norm")?;
            let name_column = arrow_string_column(&batch, 5, "name_norm")?;
            let recall_column = arrow_string_column(&batch, 6, "recall_doc")?;
            let (batch_docs, batch_rows): (Vec<_>, Vec<_>) = (0..batch.num_rows())
                .into_par_iter()
                .filter_map(|row_index| {
                    let doc = MetadataBm25Document::from_text(recall_column.value(row_index))?;
                    let row = RecallRow {
                        feature_rowid: rowid_column.value(row_index),
                        contract_address: contract_column.value(row_index).to_owned(),
                        token_id: token_column.value(row_index).to_owned(),
                        token_uri_norm: token_uri_column.value(row_index).to_owned(),
                        image_uri_norm: image_uri_column.value(row_index).to_owned(),
                        name_norm: name_column.value(row_index).to_owned(),
                        metadata_recall_match: true,
                    };
                    Some((doc, row))
                })
                .unzip();
            docs.extend(batch_docs);
            candidates.extend(batch_rows.into_iter().map(|row| MetadataRecallCandidate {
                row,
                sketch: MetadataSketch::default(),
            }));
        }
        let mut corpus_builder = CompactMetadataBm25CorpusBuilder::default();
        for doc in &docs {
            corpus_builder.add_tokens(doc.tokens());
        }
        let compact_corpus = corpus_builder.finish();
        let compact_documents = docs
            .par_iter()
            .map(|doc| compact_corpus.compact_document(doc))
            .collect();
        candidates
            .par_iter_mut()
            .zip(docs.par_iter())
            .for_each(|(candidate, doc)| {
                candidate.sketch = metadata_sketch_from_compact_corpus(doc, &compact_corpus);
            });
        drop(docs);
        let source_index = Self::build_metadata_source_index(&candidates);
        Ok(MetadataRecallIndex {
            candidates,
            compact_corpus,
            compact_documents,
            source_index,
        })
    }

    pub(super) fn cached_metadata_recall_index(
        &self,
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<Arc<MetadataRecallIndex>, AppError> {
        if let Some(index) = self.metadata_recall_index_cache()?.get(chain).cloned() {
            return Ok(index);
        }

        let index = Arc::new(Self::load_metadata_recall_index(
            conn,
            chain,
            prepared_recall_state,
        )?);
        self.metadata_recall_index_cache()?
            .insert(chain.to_string(), Arc::clone(&index));
        Ok(index)
    }

    pub(super) fn estimate_metadata_source_bucket_hits(
        seed_sketch: &MetadataSketch,
        source_index: &MetadataSourceIndex,
        hamming_threshold: u32,
        cap: usize,
    ) -> usize {
        if cap == 0 {
            return 0;
        }
        let mut seen = HashSet::new();
        for anchor in &seed_sketch.anchors {
            if let Some(indices) = source_index.anchor_indices.get(anchor) {
                for index in indices {
                    seen.insert(*index);
                    if seen.len() >= cap {
                        return cap;
                    }
                }
            }
        }
        let band_radius = hamming_threshold / METADATA_SIMHASH_BAND_COUNT as u32;
        for band_index in 0..METADATA_SIMHASH_BAND_COUNT {
            let seed_band = metadata_simhash_band_value(seed_sketch.simhash, band_index);
            for band_value in 0..METADATA_SIMHASH_BAND_VALUES {
                let band_value = band_value as u8;
                if (seed_band ^ band_value).count_ones() > band_radius {
                    continue;
                }
                let band_key = metadata_simhash_band_key(band_index, band_value);
                if let Some(indices) = source_index.simhash_band_indices.get(band_key) {
                    for index in indices {
                        seen.insert(*index);
                        if seen.len() >= cap {
                            return cap;
                        }
                    }
                }
            }
        }
        seen.len()
    }

    pub(super) fn metadata_source_candidate_indices(
        seed_sketch: &MetadataSketch,
        metadata_index: &MetadataRecallIndex,
        seed_contracts: &HashSet<String>,
    ) -> Vec<usize> {
        if seed_sketch.simhash == 0 && seed_sketch.anchors.is_empty() {
            return Vec::new();
        }
        let use_full_scan = metadata_index.source_index.anchor_indices.is_empty()
            || metadata_index
                .source_index
                .simhash_band_indices
                .iter()
                .all(Vec::is_empty)
            || Self::estimate_metadata_source_bucket_hits(
                seed_sketch,
                &metadata_index.source_index,
                METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
                metadata_index.candidates.len(),
            ) >= metadata_index.candidates.len();

        if use_full_scan {
            return metadata_index
                .candidates
                .iter()
                .enumerate()
                .filter_map(|(index, candidate)| {
                    (!seed_contracts.contains(&candidate.row.contract_address)
                        && metadata_sketch_source_match(
                            seed_sketch,
                            &candidate.sketch,
                            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
                        ))
                    .then_some(index)
                })
                .collect();
        }

        let mut seen = HashSet::new();
        let mut indices = Vec::new();
        for anchor in &seed_sketch.anchors {
            let Some(anchor_indices) = metadata_index.source_index.anchor_indices.get(anchor)
            else {
                continue;
            };
            for index in anchor_indices {
                Self::push_metadata_source_candidate_index(
                    *index,
                    seed_sketch,
                    metadata_index,
                    seed_contracts,
                    &mut seen,
                    &mut indices,
                );
            }
        }
        let band_radius =
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD / METADATA_SIMHASH_BAND_COUNT as u32;
        for band_index in 0..METADATA_SIMHASH_BAND_COUNT {
            let seed_band = metadata_simhash_band_value(seed_sketch.simhash, band_index);
            for band_value in 0..METADATA_SIMHASH_BAND_VALUES {
                let band_value = band_value as u8;
                if (seed_band ^ band_value).count_ones() > band_radius {
                    continue;
                }
                let band_key = metadata_simhash_band_key(band_index, band_value);
                let Some(bucket_indices) = metadata_index
                    .source_index
                    .simhash_band_indices
                    .get(band_key)
                else {
                    continue;
                };
                for index in bucket_indices {
                    Self::push_metadata_source_candidate_index(
                        *index,
                        seed_sketch,
                        metadata_index,
                        seed_contracts,
                        &mut seen,
                        &mut indices,
                    );
                }
            }
        }
        indices.sort_unstable();
        indices
    }

    pub(super) fn push_metadata_source_candidate_index(
        index: usize,
        seed_sketch: &MetadataSketch,
        metadata_index: &MetadataRecallIndex,
        seed_contracts: &HashSet<String>,
        seen: &mut HashSet<usize>,
        output: &mut Vec<usize>,
    ) {
        if !seen.insert(index) {
            return;
        }
        let Some(candidate) = metadata_index.candidates.get(index) else {
            return;
        };
        if seed_contracts.contains(&candidate.row.contract_address) {
            return;
        }
        if metadata_sketch_source_match(
            seed_sketch,
            &candidate.sketch,
            METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
        ) {
            output.push(index);
        }
    }

}
