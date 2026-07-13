use super::*;

type NameTokenId = u32;
type NameRowIndex = u32;
pub(super) const NAME_CANDIDATE_SCRATCH_BUDGET_BYTES: usize = ANALYSIS_NAME_SCRATCH_BUDGET_BYTES;

struct IndexedNameRecallDocument {
    char_len: usize,
    sorted_tokens: Vec<NameTokenId>,
}

pub(super) struct NameRecallRow {
    pub(super) feature_rowid: i64,
    pub(super) contract_address: String,
    pub(super) name_norm: String,
}

impl NameRecallRow {
    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.contract_address.capacity())
            .saturating_add(self.name_norm.capacity())
    }
}

pub(super) struct NameRecallIndex {
    pub(super) rows: Vec<NameRecallRow>,
    documents: Vec<IndexedNameRecallDocument>,
    token_ids: HashMap<(char, u32), NameTokenId>,
    postings: Vec<Vec<NameRowIndex>>,
    distinct_lengths: Vec<usize>,
}

enum NameSeenScratch {
    Dense {
        generations: Vec<u16>,
        generation: u16,
    },
    Sparse(HashSet<NameRowIndex>),
}

pub(super) struct NameCandidateScratch {
    candidates: Vec<NameRowIndex>,
    seen: NameSeenScratch,
    query_occurrences: HashMap<char, u32>,
    query_tokens: Vec<Option<NameTokenId>>,
    sorted_query_tokens: Vec<NameTokenId>,
}

impl NameCandidateScratch {
    fn new(row_count: usize, dense: bool) -> Self {
        if dense {
            Self::new_dense(row_count)
        } else {
            Self::new_sparse()
        }
    }

    pub(super) fn new_dense(row_count: usize) -> Self {
        Self {
            candidates: Vec::new(),
            seen: NameSeenScratch::Dense {
                generations: vec![0; row_count],
                generation: 0,
            },
            query_occurrences: HashMap::new(),
            query_tokens: Vec::new(),
            sorted_query_tokens: Vec::new(),
        }
    }

    pub(super) fn new_sparse() -> Self {
        Self {
            candidates: Vec::new(),
            seen: NameSeenScratch::Sparse(HashSet::new()),
            query_occurrences: HashMap::new(),
            query_tokens: Vec::new(),
            sorted_query_tokens: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.candidates.clear();
        self.query_occurrences.clear();
        self.query_tokens.clear();
        self.sorted_query_tokens.clear();
        match &mut self.seen {
            NameSeenScratch::Dense {
                generations,
                generation,
            } => {
                *generation = generation.wrapping_add(1);
                if *generation == 0 {
                    generations.fill(0);
                    *generation = 1;
                }
            }
            NameSeenScratch::Sparse(seen) => seen.clear(),
        }
    }

    fn push_once(&mut self, row_index: NameRowIndex) {
        let is_new = match &mut self.seen {
            NameSeenScratch::Dense {
                generations,
                generation,
            } => {
                let slot = &mut generations[row_index as usize];
                if *slot == *generation {
                    false
                } else {
                    *slot = *generation;
                    true
                }
            }
            NameSeenScratch::Sparse(seen) => seen.insert(row_index),
        };
        if is_new {
            self.candidates.push(row_index);
        }
    }
}

impl NameRecallIndex {
    pub(super) fn new(rows: Vec<NameRecallRow>) -> Result<Self, AppError> {
        if rows.len() > u32::MAX as usize {
            return Err(AppError::InvalidData(
                "name recall row count exceeds compact u32 indexes".to_string(),
            ));
        }
        let mut token_ids = HashMap::<(char, u32), NameTokenId>::new();
        let mut postings = Vec::<Vec<NameRowIndex>>::new();
        let mut documents = Vec::with_capacity(rows.len());
        let mut distinct_lengths = BTreeSet::new();
        for (row_index, row) in rows.iter().enumerate() {
            let compact_row_index = row_index as NameRowIndex;
            let mut occurrences = HashMap::<char, u32>::new();
            let mut tokens = Vec::new();
            for character in row.name_norm.chars() {
                let occurrence = occurrences.entry(character).or_default();
                let token_key = (character, *occurrence);
                *occurrence = occurrence.saturating_add(1);
                let token_id = match token_ids.get(&token_key).copied() {
                    Some(token_id) => token_id,
                    None => {
                        let token_id = u32::try_from(token_ids.len()).map_err(|_| {
                            AppError::InvalidData(
                                "name recall token dictionary exceeds compact u32 indexes"
                                    .to_string(),
                            )
                        })?;
                        token_ids.insert(token_key, token_id);
                        postings.push(Vec::new());
                        token_id
                    }
                };
                postings[token_id as usize].push(compact_row_index);
                tokens.push(token_id);
            }
            tokens.sort_unstable();
            let char_len = row.name_norm.chars().count();
            distinct_lengths.insert(char_len);
            documents.push(IndexedNameRecallDocument {
                char_len,
                sorted_tokens: tokens,
            });
        }
        Ok(Self {
            rows,
            documents,
            token_ids,
            postings,
            distinct_lengths: distinct_lengths.into_iter().collect(),
        })
    }

    pub(super) fn memory_bytes(&self) -> usize {
        let row_bytes = self
            .rows
            .iter()
            .map(NameRecallRow::memory_bytes)
            .sum::<usize>();
        let document_bytes = self
            .documents
            .iter()
            .map(|document| {
                std::mem::size_of::<IndexedNameRecallDocument>().saturating_add(
                    document
                        .sorted_tokens
                        .capacity()
                        .saturating_mul(std::mem::size_of::<NameTokenId>()),
                )
            })
            .sum::<usize>();
        let posting_bytes = self
            .postings
            .iter()
            .map(|posting| {
                std::mem::size_of::<Vec<NameRowIndex>>().saturating_add(
                    posting
                        .capacity()
                        .saturating_mul(std::mem::size_of::<NameRowIndex>()),
                )
            })
            .sum::<usize>();
        std::mem::size_of::<Self>()
            .saturating_add(row_bytes)
            .saturating_add(document_bytes)
            .saturating_add(
                self.token_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<((char, u32), NameTokenId)>()),
            )
            .saturating_add(posting_bytes)
            .saturating_add(
                self.distinct_lengths
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
    }

    pub(super) fn candidates_for_query<'a>(
        &self,
        query: &str,
        threshold: f64,
        scratch: &'a mut NameCandidateScratch,
    ) -> &'a [NameRowIndex] {
        scratch.clear();
        if query.is_empty() || threshold.is_nan() || threshold > 100.0 {
            return &scratch.candidates;
        }
        let query_len = query.chars().count();
        let Some(minimum_overlap) = self
            .distinct_lengths
            .iter()
            .copied()
            .filter(|candidate_len| {
                name_pair_lengths_can_reach_threshold(query_len, *candidate_len, threshold)
            })
            .map(|candidate_len| minimum_name_char_overlap(query_len, candidate_len, threshold))
            .min()
        else {
            return &scratch.candidates;
        };

        scratch.query_tokens.reserve(query_len);
        for character in query.chars() {
            let occurrence = scratch.query_occurrences.entry(character).or_default();
            scratch
                .query_tokens
                .push(self.token_ids.get(&(character, *occurrence)).copied());
            *occurrence = occurrence.saturating_add(1);
        }
        scratch.query_tokens.sort_unstable_by(|left, right| {
            let posting_len = |token: &Option<NameTokenId>| {
                token.map_or(0, |token_id| self.postings[token_id as usize].len())
            };
            posting_len(left)
                .cmp(&posting_len(right))
                .then_with(|| left.cmp(right))
        });

        if minimum_overlap == 0 {
            for row_index in 0..self.rows.len() {
                let document = &self.documents[row_index];
                if name_pair_lengths_can_reach_threshold(query_len, document.char_len, threshold) {
                    scratch.push_once(row_index as NameRowIndex);
                }
            }
        } else {
            let prefix_len = scratch
                .query_tokens
                .len()
                .saturating_sub(minimum_overlap)
                .saturating_add(1)
                .min(scratch.query_tokens.len());
            for token_index in 0..prefix_len {
                let Some(token_id) = scratch.query_tokens[token_index] else {
                    continue;
                };
                for &row_index in &self.postings[token_id as usize] {
                    scratch.push_once(row_index);
                }
            }
        }

        scratch
            .sorted_query_tokens
            .extend(scratch.query_tokens.iter().flatten().copied());
        scratch.sorted_query_tokens.sort_unstable();
        let sorted_query_tokens = &scratch.sorted_query_tokens;
        scratch.candidates.retain(|row_index| {
            let document = &self.documents[*row_index as usize];
            let required_overlap =
                minimum_name_char_overlap(query_len, document.char_len, threshold);
            name_pair_lengths_can_reach_threshold(query_len, document.char_len, threshold)
                && required_overlap <= query_len.min(document.char_len)
                && sorted_name_token_overlap(sorted_query_tokens, &document.sorted_tokens)
                    >= required_overlap
        });
        scratch.candidates.sort_unstable();
        &scratch.candidates
    }
}

impl DuckDbFeatureStore {
    fn estimate_name_recall_index_bytes(conn: &Connection, chain: &str) -> Result<usize, AppError> {
        let (row_count, text_chars): (i64, i64) = conn.query_row(
            &format!(
                "SELECT CAST(count(*) AS BIGINT),
                        CAST(coalesce(sum(length(coalesce(contract_address, ''))
                                          + length(coalesce(name_norm, ''))), 0) AS BIGINT)
                 FROM {NAME_RECALL_ROW_TABLE}
                 WHERE chain = ?
                   AND trim(coalesce(name_norm, '')) <> ''"
            ),
            params![chain],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let row_count = usize::try_from(row_count.max(0)).unwrap_or(usize::MAX);
        let text_chars = usize::try_from(text_chars.max(0)).unwrap_or(usize::MAX);
        Ok(row_count
            .saturating_mul(256)
            .saturating_add(text_chars.saturating_mul(48)))
    }

    pub(super) fn load_name_recall_index(
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
        memory_budget_bytes: usize,
    ) -> Result<NameRecallIndex, AppError> {
        if !prepared_recall_state.ready {
            return Err(AppError::InvalidData(format!(
                "prepared recall tables are required before loading name recall index for chain {chain:?}"
            )));
        }
        let sql = format!(
            "SELECT feature_rowid, contract_address, name_norm
             FROM {NAME_RECALL_ROW_TABLE}
             WHERE chain = ?
               AND trim(coalesce(name_norm, '')) <> ''"
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = Vec::new();
        let mut estimated_build_bytes = 0usize;
        for batch in stmt.query_arrow(params![chain])? {
            let rowid_column = arrow_i64_column(&batch, 0, "feature_rowid")?;
            let contract_column = arrow_string_column(&batch, 1, "contract_address")?;
            let name_column = arrow_string_column(&batch, 2, "name_norm")?;
            for row_index in 0..batch.num_rows() {
                let row = NameRecallRow {
                    feature_rowid: rowid_column.value(row_index),
                    contract_address: contract_column.value(row_index).to_owned(),
                    name_norm: name_column.value(row_index).to_owned(),
                };
                let character_count = row.name_norm.chars().count();
                estimated_build_bytes = estimated_build_bytes
                    .saturating_add(row.memory_bytes())
                    .saturating_add(std::mem::size_of::<IndexedNameRecallDocument>())
                    .saturating_add(character_count.saturating_mul(
                        std::mem::size_of::<NameTokenId>()
                            + std::mem::size_of::<NameRowIndex>()
                            + std::mem::size_of::<((char, u32), NameTokenId)>(),
                    ));
                rows.push(row);
            }
            if estimated_build_bytes > memory_budget_bytes {
                return Err(AppError::ResourceLimit(format!(
                    "name recall index build for chain {chain:?} requires at least {estimated_build_bytes} bytes and exceeds its configured {memory_budget_bytes}-byte budget"
                )));
            }
        }
        let index = NameRecallIndex::new(rows)?;
        let index_bytes = index.memory_bytes();
        if index_bytes > memory_budget_bytes {
            return Err(AppError::ResourceLimit(format!(
                "name recall index for chain {chain:?} requires approximately {index_bytes} bytes and exceeds its configured {memory_budget_bytes}-byte budget"
            )));
        }
        Ok(index)
    }

    pub(super) fn cached_name_recall_index(
        &self,
        conn: &Connection,
        chain: &str,
        prepared_recall_state: PreparedRecallState,
    ) -> Result<Arc<ManagedRecallIndex<NameRecallIndex>>, AppError> {
        if let Some(index) = self.name_recall_index_cache()?.get(chain) {
            return Ok(index);
        }
        let _build_guard = self
            .recall_index_build_lock
            .lock()
            .map_err(|err| AppError::DuckDb(format!("recall index build lock poisoned: {err}")))?;
        {
            let mut cache = self.name_recall_index_cache()?;
            if let Some(index) = cache.get(chain) {
                return Ok(index);
            }
        }
        let category = format!("name recall index build for chain {chain:?}");
        let estimated_bytes = Self::estimate_name_recall_index_bytes(conn, chain)?;
        let mut lease = self.reserve_recall_index_build(&category, estimated_bytes)?;
        let memory_budget_bytes = lease.bytes();
        let value =
            Self::load_name_recall_index(conn, chain, prepared_recall_state, memory_budget_bytes)?;
        let index_bytes = value.memory_bytes();
        lease.resize(
            &format!("name recall index for chain {chain:?}"),
            index_bytes,
        )?;
        let index = Arc::new(ManagedRecallIndex {
            value,
            _lease: lease,
        });
        if !self.name_recall_index_cache()?.insert(
            chain.to_string(),
            Arc::clone(&index),
            index_bytes,
        ) {
            return Err(AppError::ResourceLimit(format!(
                "name recall index for chain {chain:?} requires approximately {index_bytes} bytes and exceeds its configured cache budget"
            )));
        }
        Ok(index)
    }

    #[cfg(test)]
    pub(super) fn score_name_recall_indexed(
        profiles: &[SeedRecallProfile],
        index: &NameRecallIndex,
        name_threshold: f64,
    ) -> Vec<NameRecallMatch> {
        let dense_scratch_bytes = index
            .rows
            .len()
            .saturating_mul(std::mem::size_of::<u16>())
            .saturating_mul(2)
            .saturating_mul(profiles.len());
        let use_dense_scratch = dense_scratch_bytes <= NAME_CANDIDATE_SCRATCH_BUDGET_BYTES;
        let mut matches = profiles
            .par_iter()
            .enumerate()
            .map(|(seed_index, profile)| {
                Self::score_name_recall_profile(profile, index, name_threshold, use_dense_scratch)
                    .into_iter()
                    .map(|row_index| NameRecallMatch {
                        row_index,
                        seed_index,
                    })
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();
        matches.sort_by_key(|matched| (matched.row_index, matched.seed_index));
        matches
    }

    pub(super) fn score_name_recall_profile(
        profile: &SeedRecallProfile,
        index: &NameRecallIndex,
        name_threshold: f64,
        use_dense_scratch: bool,
    ) -> Vec<usize> {
        let mut query_scratch = NameCandidateScratch::new(index.rows.len(), use_dense_scratch);
        let mut profile_scratch = NameCandidateScratch::new(index.rows.len(), use_dense_scratch);
        // Start a non-zero dense generation before merging query candidates.
        profile_scratch.clear();
        for query in &profile.seed_name_norms {
            for &row_index in index.candidates_for_query(query, name_threshold, &mut query_scratch)
            {
                profile_scratch.push_once(row_index);
            }
        }
        profile_scratch
            .candidates
            .par_iter()
            .filter_map(|row_index| {
                let row_index = *row_index as usize;
                let row = &index.rows[row_index];
                if row.name_norm.is_empty()
                    || profile.seed_contracts.contains(&row.contract_address)
                {
                    return None;
                }
                profile
                    .seed_name_queries
                    .iter()
                    .any(|query| {
                        query
                            .score_percent(&row.name_norm, name_threshold)
                            .is_some()
                    })
                    .then_some(row_index)
            })
            .collect()
    }
}

fn minimum_name_char_overlap(left_len: usize, right_len: usize, threshold: f64) -> usize {
    if threshold.is_nan() || threshold > 100.0 {
        return left_len.min(right_len).saturating_add(1);
    }
    if threshold <= 0.0 {
        return 0;
    }
    let max_overlap = left_len.min(right_len);
    let mut low = 0usize;
    let mut high = max_overlap.saturating_add(1);
    while low < high {
        let middle = low + (high - low) / 2;
        if optimistic_jaro_winkler_from_overlap(left_len, right_len, middle) >= threshold {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

fn optimistic_jaro_winkler_from_overlap(left_len: usize, right_len: usize, overlap: usize) -> f64 {
    if left_len == 0 || right_len == 0 || overlap == 0 {
        return 0.0;
    }
    let overlap = overlap.min(left_len).min(right_len) as f64;
    let jaro = (overlap / left_len as f64 + overlap / right_len as f64 + 1.0) / 3.0;
    let prefix = overlap.min(left_len.min(right_len).min(4) as f64);
    let similarity = if jaro > 0.7 {
        jaro + 0.1 * prefix * (1.0 - jaro)
    } else {
        jaro
    };
    similarity.min(1.0) * 100.0
}

fn sorted_name_token_overlap(left: &[NameTokenId], right: &[NameTokenId]) -> usize {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut overlap = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => {
                overlap += 1;
                left_index += 1;
                right_index += 1;
            }
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    overlap
}

fn name_pair_lengths_can_reach_threshold(
    left_len: usize,
    right_len: usize,
    threshold: f64,
) -> bool {
    jaro_winkler_upper_bound_from_lengths(left_len, right_len) >= threshold
}

fn jaro_winkler_upper_bound_from_lengths(left_len: usize, right_len: usize) -> f64 {
    if left_len == 0 || right_len == 0 {
        return if left_len == right_len { 100.0 } else { 0.0 };
    }
    let shorter = left_len.min(right_len) as f64;
    let longer = left_len.max(right_len) as f64;
    let max_jaro = (1.0 + shorter / longer + 1.0) / 3.0;
    let max_prefix = left_len.min(right_len).min(4) as f64;
    let max_winkler = max_jaro + 0.1 * max_prefix * (1.0 - max_jaro);
    max_winkler.min(1.0) * 100.0
}
