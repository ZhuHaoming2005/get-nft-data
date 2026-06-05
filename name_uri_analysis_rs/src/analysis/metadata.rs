use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

const METADATA_THRESHOLD: f64 = 0.6;
const METADATA_MATCH_MODE: &str = "bm25_representative";
const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;
const METADATA_BM25_K1: f64 = 1.2;
const METADATA_BM25_B: f64 = 0.75;
const METADATA_LOAD_CHUNK_ROWS: usize = 16 * 1024;
const METADATA_PAIR_LEFT_CHUNK_SIZE: usize = 256;
type MetadataDocKey = String;
type MetadataContractIndex = u32;
type MetadataDocIndex = u32;

#[derive(Debug)]
struct RawMetadataRow {
    chain: String,
    contract_address: String,
    metadata_json: String,
    nft_count: i64,
}

#[derive(Debug)]
struct IndexedMetadataRow {
    chain_index: usize,
    contract_address: String,
    nft_count: i64,
    doc: MetadataBm25Document,
    doc_key: MetadataDocKey,
}

#[derive(Clone, Debug)]
struct MetadataContract {
    chain_index: usize,
    nft_count: i64,
}

#[derive(Debug)]
struct SourceMetadataDocEntry {
    doc: MetadataBm25Document,
    contracts: Vec<MetadataContractIndex>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
}

#[derive(Debug)]
struct MetadataDocEntry {
    contracts: Vec<MetadataContractIndex>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
}

#[derive(Debug)]
struct MetadataData {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    docs: Vec<MetadataDocEntry>,
    metadata_index: InternedMetadataIndex,
}

struct MetadataDataBuilder {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    contract_index_by_key: HashMap<(usize, String), usize>,
    docs: Vec<SourceMetadataDocEntry>,
    doc_index_by_key: HashMap<MetadataDocKey, usize>,
    chain_count: usize,
}

#[derive(Debug, Clone)]
struct MetadataBm25Document {
    tokens: Vec<String>,
    unique_tokens: Vec<String>,
}

#[derive(Debug)]
struct InternedMetadataDoc {
    unique_tokens: Vec<usize>,
}

#[derive(Debug)]
struct InternedMetadataSourceDoc {
    tokens: Vec<usize>,
    term_freqs: HashMap<usize, usize>,
    unique_tokens: Vec<usize>,
}

#[derive(Debug)]
struct InternedMetadataCorpus {
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: Vec<usize>,
}

#[derive(Debug)]
struct PreparedInternedMetadataQuery {
    terms: Vec<(usize, usize)>,
    denominator: f64,
}

#[derive(Debug)]
struct PreparedInternedMetadataDoc {
    token_weights: Vec<(usize, f64)>,
}

#[derive(Debug)]
struct InternedMetadataIndex {
    docs: Vec<InternedMetadataDoc>,
    corpus: InternedMetadataCorpus,
    queries: Vec<PreparedInternedMetadataQuery>,
    prepared_docs: Vec<PreparedInternedMetadataDoc>,
    postings: Vec<Vec<MetadataDocIndex>>,
    #[cfg(test)]
    token_ids: HashMap<String, usize>,
}

struct MetadataCandidateScratch {
    seen_generation: Vec<u16>,
    generation: u16,
    candidates: Vec<MetadataDocIndex>,
}

struct MetadataCandidateScratchPool {
    doc_count: usize,
    scratches: Mutex<Vec<MetadataCandidateScratch>>,
}

struct MetadataPairScoringContext<'a> {
    docs: &'a [InternedMetadataDoc],
    corpus: &'a InternedMetadataCorpus,
    postings: &'a [Vec<MetadataDocIndex>],
    queries: &'a [PreparedInternedMetadataQuery],
    prepared_docs: &'a [PreparedInternedMetadataDoc],
}

struct MetadataUnionState {
    intra: UnionFind,
    cross: Option<SparseUnionFind>,
    chain_matrix: Option<Vec<SparseUnionFind>>,
}

#[derive(Default)]
struct MetadataComponentAccumulator {
    primary_contract_count: i64,
    primary_nft_count: i64,
    total_contract_count: i64,
    first_chain: Option<usize>,
    has_secondary: bool,
}

#[derive(Default)]
struct MetadataPairComponentAccumulator {
    left_contract_count: i64,
    left_nft_count: i64,
    right_contract_count: i64,
    right_nft_count: i64,
    total_contract_count: i64,
}

impl MetadataDataBuilder {
    fn new(chain_count: usize) -> Self {
        Self {
            contracts: Vec::new(),
            contracts_by_chain: vec![Vec::new(); chain_count],
            contract_index_by_key: HashMap::new(),
            docs: Vec::new(),
            doc_index_by_key: HashMap::new(),
            chain_count,
        }
    }

    fn merge_indexed_rows(&mut self, indexed_rows: Vec<IndexedMetadataRow>) {
        for row in indexed_rows {
            self.merge_indexed_row(row);
        }
    }

    fn merge_indexed_row(&mut self, row: IndexedMetadataRow) {
        let contract_key = (row.chain_index, row.contract_address);
        let contract_index = match self.contract_index_by_key.get(&contract_key).copied() {
            Some(index) => index,
            None => {
                let index = self.contracts.len();
                self.contract_index_by_key.insert(contract_key, index);
                self.contracts.push(MetadataContract {
                    chain_index: row.chain_index,
                    nft_count: row.nft_count,
                });
                self.contracts_by_chain[row.chain_index]
                    .push(metadata_contract_index_from_usize(index));
                index
            }
        };

        let doc_index = match self.doc_index_by_key.get(&row.doc_key).copied() {
            Some(index) => index,
            None => {
                let index = self.docs.len();
                self.doc_index_by_key.insert(row.doc_key, index);
                self.docs.push(SourceMetadataDocEntry {
                    doc: row.doc,
                    contracts: Vec::new(),
                    contracts_by_chain: vec![Vec::new(); self.chain_count],
                });
                index
            }
        };

        let compact_contract_index = metadata_contract_index_from_usize(contract_index);
        self.docs[doc_index].contracts.push(compact_contract_index);
        self.docs[doc_index].contracts_by_chain[row.chain_index].push(compact_contract_index);
    }

    fn finish(self) -> MetadataData {
        let (metadata_index, docs) = InternedMetadataIndex::from_source_doc_entries(self.docs);
        MetadataData {
            contracts: self.contracts,
            contracts_by_chain: self.contracts_by_chain,
            docs,
            metadata_index,
        }
    }
}

impl MetadataCandidateScratch {
    fn new(doc_count: usize) -> Self {
        Self {
            seen_generation: vec![0; doc_count],
            generation: 0,
            candidates: Vec::new(),
        }
    }

    fn clear_for_next_left(&mut self) {
        self.candidates.clear();
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen_generation.fill(0);
            self.generation = 1;
        }
    }

    fn push_once(&mut self, index: MetadataDocIndex) {
        let index_usize = metadata_doc_index_to_usize(index);
        if self.seen_generation[index_usize] == self.generation {
            return;
        }
        self.seen_generation[index_usize] = self.generation;
        self.candidates.push(index);
    }
}

impl MetadataCandidateScratchPool {
    fn new(doc_count: usize) -> Self {
        Self {
            doc_count,
            scratches: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> MetadataCandidateScratch {
        self.scratches
            .lock()
            .expect("metadata candidate scratch pool lock poisoned")
            .pop()
            .unwrap_or_else(|| MetadataCandidateScratch::new(self.doc_count))
    }

    fn put(&self, scratch: MetadataCandidateScratch) {
        self.scratches
            .lock()
            .expect("metadata candidate scratch pool lock poisoned")
            .push(scratch);
    }
}

fn run_metadata_analysis(
    conn: &Connection,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    threads: usize,
    progress: &ProgressTracker,
) -> Result<Vec<SummaryRow>, AnalysisError> {
    progress.start_phase("analyzing metadata duplicates", 3);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .map_err(|err| AnalysisError::InvalidData(err.to_string()))?;
    let data = load_metadata_data(conn, chains, &pool)?;
    progress.step(format!(
        "loaded {} metadata documents for {} contracts",
        data.docs.len(),
        data.contracts.len()
    ));
    let mut rows = Vec::new();
    if data.contracts.len() < 2 || data.docs.is_empty() {
        push_empty_metadata_rows(&mut rows, chains, totals);
        progress.step("metadata scoring skipped");
        progress.step("metadata rows summarized");
        progress.finish_phase("metadata analysis complete");
        return Ok(rows);
    }

    let mut state = MetadataUnionState {
        intra: UnionFind::new(data.contracts.len()),
        cross: (chains.len() > 1).then(SparseUnionFind::default),
        chain_matrix: (chains.len() > 1)
            .then(|| new_chain_matrix_reuse_states(chain_pair_count(chains.len()))),
    };
    pool.install(|| union_metadata_pairs(&data, chains.len(), &mut state, progress));
    progress.step("metadata documents scored");
    push_metadata_summary_rows(&mut rows, &data, chains, totals, &mut state);
    progress.step("metadata rows summarized");
    progress.finish_phase("metadata analysis complete");
    Ok(rows)
}

fn load_metadata_data(
    conn: &Connection,
    chains: &[String],
    pool: &rayon::ThreadPool,
) -> Result<MetadataData, AnalysisError> {
    let chain_indexes = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt = conn.prepare(&metadata_raw_rows_sql())?;
    let rows = stmt.query_map([], |row| {
        Ok(RawMetadataRow {
            chain: row.get::<_, String>(0)?,
            contract_address: row.get::<_, String>(1)?,
            metadata_json: row.get::<_, String>(2)?,
            nft_count: row.get::<_, i64>(3)?,
        })
    })?;

    let mut builder = MetadataDataBuilder::new(chains.len());
    let mut raw_rows = Vec::with_capacity(METADATA_LOAD_CHUNK_ROWS);

    for row in rows {
        raw_rows.push(row?);
        if raw_rows.len() >= METADATA_LOAD_CHUNK_ROWS {
            let chunk = std::mem::replace(
                &mut raw_rows,
                Vec::with_capacity(METADATA_LOAD_CHUNK_ROWS),
            );
            builder.merge_indexed_rows(pool.install(|| {
                index_metadata_raw_row_chunk(chunk, &chain_indexes)
            }));
        }
    }

    if !raw_rows.is_empty() {
        builder.merge_indexed_rows(pool.install(|| {
            index_metadata_raw_row_chunk(raw_rows, &chain_indexes)
        }));
    }

    Ok(builder.finish())
}

fn metadata_raw_rows_sql() -> String {
    format!(
        "
            WITH totals AS (
                SELECT chain, contract_address, count(*)::BIGINT AS nft_count
                FROM analysis_rows
                WHERE contract_address <> ''
                GROUP BY chain, contract_address
            ),
            eligible_metadata AS (
                SELECT rowid AS metadata_row_id,
                       chain,
                       contract_address,
                       metadata_json
                FROM analysis_rows
                WHERE contract_address <> ''
                  AND metadata_json <> ''
                  AND length(metadata_json) <= {MAX_METADATA_BYTES_FOR_DEDUP}
                  AND (
                      starts_with(metadata_json, '{{')
                      OR starts_with(metadata_json, '[')
                  )
            ),
            first_metadata AS (
                SELECT chain,
                       contract_address,
                       min(metadata_row_id) AS metadata_row_id,
                       count(*)::BIGINT AS metadata_count
                FROM eligible_metadata
                GROUP BY chain, contract_address
            )
            SELECT m.chain,
                   m.contract_address,
                   m.metadata_json,
                   t.nft_count,
                   f.metadata_count
            FROM first_metadata f
            JOIN eligible_metadata m
              ON m.chain = f.chain
             AND m.contract_address = f.contract_address
             AND m.metadata_row_id = f.metadata_row_id
            JOIN totals t
              ON t.chain = m.chain
             AND t.contract_address = m.contract_address
            "
    )
}

fn index_metadata_raw_row_chunk(
    raw_rows: Vec<RawMetadataRow>,
    chain_indexes: &HashMap<&str, usize>,
) -> Vec<IndexedMetadataRow> {
    raw_rows
        .into_par_iter()
        .filter_map(|row| {
            let chain_index = chain_indexes.get(row.chain.as_str()).copied()?;
            let document = metadata_document_from_json(&row.metadata_json);
            let doc = MetadataBm25Document::from_text(&document)?;
            if !metadata_document_has_informative_token(&doc) {
                return None;
            }
            let doc_key = metadata_document_key(&document);
            Some(IndexedMetadataRow {
                chain_index,
                contract_address: row.contract_address,
                nft_count: row.nft_count,
                doc,
                doc_key,
            })
        })
        .collect()
}

fn metadata_document_key(document: &str) -> MetadataDocKey {
    document.to_string()
}

fn metadata_document_has_informative_token(doc: &MetadataBm25Document) -> bool {
    doc.unique_tokens
        .iter()
        .any(|token| !is_metadata_schema_key_token(token))
}

fn is_metadata_schema_key_token(token: &str) -> bool {
    matches!(
        token,
        "about"
            | "animation_url"
            | "attributes"
            | "bio"
            | "chain"
            | "collection"
            | "compiler"
            | "contract"
            | "creator"
            | "creators"
            | "description"
            | "display_type"
            | "external_url"
            | "fee_recipient"
            | "image"
            | "image_url"
            | "license"
            | "lore"
            | "marketplace"
            | "name"
            | "royalties"
            | "royalty"
            | "seller_fee_basis_points"
            | "story"
            | "summary"
            | "trait_type"
            | "value"
    )
}

fn union_metadata_pairs(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    progress: &ProgressTracker,
) {
    for doc_index in 0..data.docs.len() {
        apply_metadata_exact_doc_unions(data, chain_count, state, doc_index);
    }

    let index = &data.metadata_index;
    if index.corpus.total_docs == 0 {
        return;
    }
    let scoring_left_count = data.docs.len().saturating_sub(1);
    let mut scored_candidate_pairs = 0u64;
    let mut scored_left_docs = 0usize;
    let mut matched_doc_pairs = 0u64;
    let progress_start = Instant::now();
    let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());
    progress.add_work(metadata_scoring_progress_units(scoring_left_count));
    progress.set_message(metadata_pair_progress_message(
        scored_candidate_pairs,
        scored_left_docs,
        scoring_left_count,
        matched_doc_pairs,
        progress_start.elapsed(),
    ));
    for left_start in (0..scoring_left_count).step_by(METADATA_PAIR_LEFT_CHUNK_SIZE) {
        let left_end = (left_start + METADATA_PAIR_LEFT_CHUNK_SIZE).min(scoring_left_count);
        let batch = collect_metadata_doc_pair_hits_for_left_range(
            left_start..left_end,
            MetadataPairScoringContext {
                docs: &index.docs,
                corpus: &index.corpus,
                postings: &index.postings,
                queries: &index.queries,
                prepared_docs: &index.prepared_docs,
            },
            &scratch_pool,
        );
        scored_candidate_pairs = scored_candidate_pairs.saturating_add(batch.candidate_pairs);
        scored_left_docs = left_end;
        matched_doc_pairs = matched_doc_pairs.saturating_add(batch.hits.len() as u64);
        progress.inc(metadata_scoring_batch_progress_units(left_start, left_end));
        progress.set_message(metadata_pair_progress_message(
            scored_candidate_pairs,
            scored_left_docs,
            scoring_left_count,
            matched_doc_pairs,
            progress_start.elapsed(),
        ));
        for (left, right) in batch.hits {
            apply_metadata_doc_pair_union(data, chain_count, state, left, right);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct MetadataDocPairBatch {
    hits: Vec<(usize, usize)>,
    candidate_pairs: u64,
}

fn metadata_scoring_progress_units(scoring_left_count: usize) -> u64 {
    scoring_left_count as u64
}

fn metadata_scoring_batch_progress_units(left_start: usize, left_end: usize) -> u64 {
    left_end.saturating_sub(left_start) as u64
}

fn metadata_pair_progress_message(
    scored_pairs: u64,
    scored_left_docs: usize,
    total_left_docs: usize,
    matched_pairs: u64,
    elapsed: Duration,
) -> String {
    let remaining_left_docs = total_left_docs.saturating_sub(scored_left_docs);
    let estimated_remaining_pairs = estimate_remaining_metadata_candidate_pairs(
        scored_pairs,
        scored_left_docs,
        remaining_left_docs,
    );
    let throughput = format_metadata_pair_throughput(scored_pairs, elapsed);
    let eta = format_metadata_pair_eta(estimated_remaining_pairs, scored_pairs, elapsed);
    format!(
        "metadata candidate pairs scored {scored_pairs}; left docs {scored_left_docs}/{total_left_docs}; estimated remaining {estimated_remaining_pairs}; throughput {throughput}; ETA {eta}; matched doc pairs {matched_pairs}"
    )
}

fn estimate_remaining_metadata_candidate_pairs(
    scored_pairs: u64,
    scored_left_docs: usize,
    remaining_left_docs: usize,
) -> u64 {
    if scored_pairs == 0 || scored_left_docs == 0 || remaining_left_docs == 0 {
        return 0;
    }
    let estimated = (scored_pairs as u128)
        .saturating_mul(remaining_left_docs as u128)
        .div_ceil(scored_left_docs as u128);
    estimated.min(u64::MAX as u128) as u64
}

fn format_metadata_pair_throughput(scored_pairs: u64, elapsed: Duration) -> String {
    let Some(pairs_per_second) = metadata_pairs_per_second(scored_pairs, elapsed) else {
        return "n/a".to_string();
    };
    format!("{pairs_per_second:.1} pairs/s")
}

fn format_metadata_pair_eta(remaining_pairs: u64, scored_pairs: u64, elapsed: Duration) -> String {
    if scored_pairs == 0 {
        return "n/a".to_string();
    }
    if remaining_pairs == 0 {
        return "0s".to_string();
    }
    let Some(pairs_per_second) = metadata_pairs_per_second(scored_pairs, elapsed) else {
        return "n/a".to_string();
    };
    format_metadata_duration(Duration::from_secs_f64(
        (remaining_pairs as f64 / pairs_per_second).ceil(),
    ))
}

fn metadata_pairs_per_second(scored_pairs: u64, elapsed: Duration) -> Option<f64> {
    if scored_pairs == 0 {
        return None;
    }
    let elapsed_seconds = elapsed.as_secs_f64();
    if elapsed_seconds <= 0.0 {
        return None;
    }
    Some(scored_pairs as f64 / elapsed_seconds)
}

fn format_metadata_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {remaining_seconds:02}s");
    }
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    format!("{hours}h {remaining_minutes:02}m")
}

fn collect_metadata_doc_pair_hits_for_left_range(
    left_range: std::ops::Range<usize>,
    context: MetadataPairScoringContext<'_>,
    scratch_pool: &MetadataCandidateScratchPool,
) -> MetadataDocPairBatch {
    let context = &context;
    let (mut hits, candidate_pairs) = left_range
        .into_par_iter()
        .map(|left| {
            let mut scratch = scratch_pool.take();
            let mut local_hits = Vec::new();
            let local_candidate_pairs = collect_metadata_doc_pair_hits_for_left_with_scratch(
                left,
                context,
                &mut scratch,
                &mut local_hits,
            );
            scratch_pool.put(scratch);
            (local_hits, local_candidate_pairs)
        })
        .reduce(
            || (Vec::new(), 0u64),
            |(mut left_hits, left_candidate_pairs), (mut right_hits, right_candidate_pairs)| {
                left_hits.append(&mut right_hits);
                (
                    left_hits,
                    left_candidate_pairs.saturating_add(right_candidate_pairs),
                )
            },
        );
    hits.sort_unstable();
    hits.dedup();
    MetadataDocPairBatch {
        hits,
        candidate_pairs,
    }
}

fn collect_metadata_doc_pair_hits_for_left_with_scratch(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &mut MetadataCandidateScratch,
    hits: &mut Vec<(usize, usize)>,
) -> u64 {
    let candidates = metadata_candidate_indices_for_left_with_scratch(
        left,
        &context.docs[left],
        context.postings,
        scratch,
    );
    for &right in candidates {
        let right = metadata_doc_index_to_usize(right);
        if metadata_pair_has_rare_anchor(left, right, context.docs, context.corpus)
            && metadata_pair_score(left, right, context.queries, context.prepared_docs)
                >= METADATA_THRESHOLD
        {
            hits.push((left, right));
        }
    }
    candidates.len() as u64
}

fn metadata_candidate_indices_for_left_with_scratch<'a>(
    left: usize,
    doc: &InternedMetadataDoc,
    postings: &[Vec<MetadataDocIndex>],
    scratch: &'a mut MetadataCandidateScratch,
) -> &'a [MetadataDocIndex] {
    scratch.clear_for_next_left();
    let left = metadata_doc_index_from_usize(left);
    for token in doc.unique_tokens() {
        let Some(indices) = postings.get(*token) else {
            continue;
        };
        let start = indices.partition_point(|&right| right <= left);
        for &right in &indices[start..] {
            scratch.push_once(right);
        }
    }
    &scratch.candidates
}

fn metadata_pair_score(
    left: usize,
    right: usize,
    queries: &[PreparedInternedMetadataQuery],
    prepared_docs: &[PreparedInternedMetadataDoc],
) -> f64 {
    let left_to_right = score_metadata_with_prepared_doc(&queries[left], &prepared_docs[right]);
    if left_to_right >= METADATA_THRESHOLD {
        return left_to_right;
    }
    score_metadata_with_prepared_doc(&queries[right], &prepared_docs[left])
}

fn metadata_pair_has_rare_anchor(
    left: usize,
    right: usize,
    docs: &[InternedMetadataDoc],
    corpus: &InternedMetadataCorpus,
) -> bool {
    let max_doc_freq = metadata_rare_anchor_max_doc_freq(corpus.total_docs);
    let left_tokens = docs[left].unique_tokens();
    let right_tokens = docs[right].unique_tokens();
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left_tokens.len() && right_index < right_tokens.len() {
        match left_tokens[left_index].cmp(&right_tokens[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                let token = left_tokens[left_index];
                if corpus
                    .doc_freqs
                    .get(token)
                    .is_some_and(|doc_freq| *doc_freq <= max_doc_freq)
                {
                    return true;
                }
                left_index += 1;
                right_index += 1;
            }
        }
    }
    false
}

fn metadata_rare_anchor_max_doc_freq(total_docs: usize) -> usize {
    total_docs.div_ceil(200).max(2)
}


fn apply_metadata_exact_doc_unions(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    doc_index: usize,
) {
    let entry = &data.docs[doc_index];
    if entry.contracts.len() < 2 {
        return;
    }

    for chain_contracts in &entry.contracts_by_chain {
        union_dense_contract_group(&mut state.intra, chain_contracts);
    }

    if let Some(cross) = &mut state.cross {
        union_sparse_contract_group_when_multiple_chains(
            cross,
            &entry.contracts,
            &entry.contracts_by_chain,
        );
    }
    if let Some(matrix) = &mut state.chain_matrix {
        for left_chain in 0..chain_count {
            for right_chain in left_chain + 1..chain_count {
                if entry.contracts_by_chain[left_chain].is_empty()
                    || entry.contracts_by_chain[right_chain].is_empty()
                {
                    continue;
                }
                let pair_index = chain_pair_index(left_chain, right_chain, chain_count);
                union_sparse_bipartite_contract_groups(
                    &mut matrix[pair_index],
                    &entry.contracts_by_chain[left_chain],
                    &entry.contracts_by_chain[right_chain],
                );
            }
        }
    }
}

fn apply_metadata_doc_pair_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left_doc: usize,
    right_doc: usize,
) {
    let left = &data.docs[left_doc];
    let right = &data.docs[right_doc];
    for chain in 0..chain_count {
        if left.contracts_by_chain[chain].is_empty() || right.contracts_by_chain[chain].is_empty()
        {
            continue;
        }
        union_dense_bipartite_contract_groups(
            &mut state.intra,
            &left.contracts_by_chain[chain],
            &right.contracts_by_chain[chain],
        );
    }

    if let Some(cross) = &mut state.cross {
        for left_chain in 0..chain_count {
            if left.contracts_by_chain[left_chain].is_empty() {
                continue;
            }
            for right_chain in 0..chain_count {
                if left_chain == right_chain || right.contracts_by_chain[right_chain].is_empty() {
                    continue;
                }
                union_sparse_bipartite_contract_groups(
                    cross,
                    &left.contracts_by_chain[left_chain],
                    &right.contracts_by_chain[right_chain],
                );
            }
        }
    }

    if let Some(matrix) = &mut state.chain_matrix {
        for left_chain in 0..chain_count {
            for right_chain in left_chain + 1..chain_count {
                let mut primary_contracts = Vec::new();
                let mut secondary_contracts = Vec::new();
                if !left.contracts_by_chain[left_chain].is_empty()
                    && !right.contracts_by_chain[right_chain].is_empty()
                {
                    primary_contracts.extend_from_slice(&left.contracts_by_chain[left_chain]);
                    secondary_contracts.extend_from_slice(&right.contracts_by_chain[right_chain]);
                }
                if !right.contracts_by_chain[left_chain].is_empty()
                    && !left.contracts_by_chain[right_chain].is_empty()
                {
                    primary_contracts.extend_from_slice(&right.contracts_by_chain[left_chain]);
                    secondary_contracts.extend_from_slice(&left.contracts_by_chain[right_chain]);
                }
                if primary_contracts.is_empty() || secondary_contracts.is_empty() {
                    continue;
                }
                let pair_index = chain_pair_index(left_chain, right_chain, chain_count);
                union_sparse_bipartite_contract_groups(
                    &mut matrix[pair_index],
                    &primary_contracts,
                    &secondary_contracts,
                );
            }
        }
    }
}

fn union_dense_contract_group(union_find: &mut UnionFind, contracts: &[MetadataContractIndex]) {
    let Some((&anchor, rest)) = contracts.split_first() else {
        return;
    };
    let anchor = metadata_contract_index_to_usize(anchor);
    for &contract in rest {
        union_find.union(anchor, metadata_contract_index_to_usize(contract));
    }
}

fn union_dense_bipartite_contract_groups(
    union_find: &mut UnionFind,
    left_contracts: &[MetadataContractIndex],
    right_contracts: &[MetadataContractIndex],
) {
    let Some(&anchor) = left_contracts.first() else {
        return;
    };
    let anchor = metadata_contract_index_to_usize(anchor);
    for &contract in &left_contracts[1..] {
        union_find.union(anchor, metadata_contract_index_to_usize(contract));
    }
    for &contract in right_contracts {
        union_find.union(anchor, metadata_contract_index_to_usize(contract));
    }
}

fn union_sparse_contract_group_when_multiple_chains(
    union_find: &mut SparseUnionFind,
    contracts: &[MetadataContractIndex],
    contracts_by_chain: &[Vec<MetadataContractIndex>],
) {
    if contracts_by_chain
        .iter()
        .filter(|contracts| !contracts.is_empty())
        .count()
        < 2
    {
        return;
    }
    let Some((&anchor, rest)) = contracts.split_first() else {
        return;
    };
    let anchor = metadata_contract_index_to_usize(anchor);
    for &contract in rest {
        union_find.union(anchor, metadata_contract_index_to_usize(contract));
    }
}

fn union_sparse_bipartite_contract_groups(
    union_find: &mut SparseUnionFind,
    left_contracts: &[MetadataContractIndex],
    right_contracts: &[MetadataContractIndex],
) {
    let Some(&anchor) = left_contracts.first() else {
        return;
    };
    let anchor = metadata_contract_index_to_usize(anchor);
    for &contract in &left_contracts[1..] {
        union_find.union(anchor, metadata_contract_index_to_usize(contract));
    }
    for &contract in right_contracts {
        union_find.union(anchor, metadata_contract_index_to_usize(contract));
    }
}

fn push_empty_metadata_rows(
    rows: &mut Vec<SummaryRow>,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
) {
    for chain in chains {
        let total = totals.get(chain).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        rows.push(metadata_summary_row(
            "intra_chain",
            chain,
            "",
            total,
            GroupSummary::default(),
        ));
        if chains.len() > 1 {
            rows.push(metadata_summary_row(
                "cross_chain_summary",
                chain,
                "",
                total,
                GroupSummary::default(),
            ));
        }
    }
    if chains.len() > 1 {
        for primary_index in 0..chains.len() {
            for secondary_index in 0..chains.len() {
                if primary_index == secondary_index {
                    continue;
                }
                let primary = &chains[primary_index];
                let total = totals.get(primary).copied().unwrap_or(NameTotals {
                    contracts: 0,
                    nfts: 0,
                });
                rows.push(metadata_summary_row(
                    "chain_matrix",
                    primary,
                    &chains[secondary_index],
                    total,
                    GroupSummary::default(),
                ));
            }
        }
    }
}

fn push_metadata_summary_rows(
    rows: &mut Vec<SummaryRow>,
    data: &MetadataData,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    state: &mut MetadataUnionState,
) {
    let mut dense_scratch = MetadataDenseComponentScratch::new(data.contracts.len());
    for (chain_index, chain) in chains.iter().enumerate() {
        let total = totals.get(chain).copied().unwrap_or(NameTotals {
            contracts: 0,
            nfts: 0,
        });
        let intra = summarize_metadata_dense_components_for_primary(
            data,
            &data.contracts_by_chain[chain_index],
            &mut state.intra,
            &mut dense_scratch,
        );
        rows.push(metadata_summary_row(
            "intra_chain",
            chain,
            "",
            total,
            intra,
        ));

        if let Some(cross) = &mut state.cross {
            let cross_summary = summarize_metadata_sparse_components_for_primary(data, cross, chain_index);
            rows.push(metadata_summary_row(
                "cross_chain_summary",
                chain,
                "",
                total,
                cross_summary,
            ));
        }
    }

    let Some(matrix) = &mut state.chain_matrix else {
        return;
    };
    for (pair_index, union_find) in matrix.iter_mut().enumerate() {
        let (left_chain, right_chain) = chain_pair_from_index(pair_index, chains.len());
        let (left_summary, right_summary) =
            summarize_metadata_sparse_components_for_chain_pair(data, union_find, left_chain, right_chain);
        push_metadata_chain_matrix_row(rows, chains, totals, left_chain, right_chain, left_summary);
        push_metadata_chain_matrix_row(rows, chains, totals, right_chain, left_chain, right_summary);
    }
}

struct MetadataDenseComponentScratch {
    components: Vec<MetadataComponentAccumulator>,
    touched_roots: Vec<usize>,
}

impl MetadataDenseComponentScratch {
    fn new(size: usize) -> Self {
        let mut components = Vec::with_capacity(size);
        components.resize_with(size, MetadataComponentAccumulator::default);
        Self {
            components,
            touched_roots: Vec::new(),
        }
    }

    fn clear_touched(&mut self) {
        for root in self.touched_roots.drain(..) {
            self.components[root] = MetadataComponentAccumulator::default();
        }
    }
}

fn summarize_metadata_dense_components_for_primary(
    data: &MetadataData,
    primary_contracts: &[MetadataContractIndex],
    union_find: &mut UnionFind,
    scratch: &mut MetadataDenseComponentScratch,
) -> GroupSummary {
    for &index in primary_contracts {
        let index = metadata_contract_index_to_usize(index);
        let contract = &data.contracts[index];
        let root = union_find.find(index);
        let component = &mut scratch.components[root];
        if component.total_contract_count == 0 && component.primary_contract_count == 0 {
            scratch.touched_roots.push(root);
        }
        component.total_contract_count += 1;
        component.primary_contract_count += 1;
        component.primary_nft_count += contract.nft_count;
    }

    let mut summary = GroupSummary::default();
    for &root in &scratch.touched_roots {
        let component = &scratch.components[root];
        if component.primary_contract_count == 0 || component.total_contract_count < 2 {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    scratch.clear_touched();
    summary
}

fn summarize_metadata_sparse_components_for_primary(
    data: &MetadataData,
    union_find: &mut SparseUnionFind,
    primary: usize,
) -> GroupSummary {
    let mut components = HashMap::<usize, MetadataComponentAccumulator>::new();
    for local_index in 0..union_find.atom_count() {
        let contract_index = union_find.atom_at(local_index);
        let contract = &data.contracts[contract_index];
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        component.total_contract_count += 1;
        match component.first_chain {
            Some(first) if first != contract.chain_index => component.has_secondary = true,
            None => component.first_chain = Some(contract.chain_index),
            _ => {}
        }
        if contract.chain_index != primary {
            component.has_secondary = true;
        } else {
            component.primary_contract_count += 1;
            component.primary_nft_count += contract.nft_count;
        }
    }

    let mut summary = GroupSummary::default();
    for component in components.values() {
        if component.primary_contract_count == 0
            || !component.has_secondary
            || component.total_contract_count < 2
        {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    summary
}

fn summarize_metadata_sparse_components_for_chain_pair(
    data: &MetadataData,
    union_find: &mut SparseUnionFind,
    left_chain: usize,
    right_chain: usize,
) -> (GroupSummary, GroupSummary) {
    let mut components = HashMap::<usize, MetadataPairComponentAccumulator>::new();
    for local_index in 0..union_find.atom_count() {
        let contract_index = union_find.atom_at(local_index);
        let contract = &data.contracts[contract_index];
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        component.total_contract_count += 1;
        if contract.chain_index == left_chain {
            component.left_contract_count += 1;
            component.left_nft_count += contract.nft_count;
        } else if contract.chain_index == right_chain {
            component.right_contract_count += 1;
            component.right_nft_count += contract.nft_count;
        }
    }

    let mut left_summary = GroupSummary::default();
    let mut right_summary = GroupSummary::default();
    for component in components.values() {
        accumulate_pair_component_summary(
            &mut left_summary,
            component.left_contract_count,
            component.left_nft_count,
            component.right_contract_count,
            component.total_contract_count,
        );
        accumulate_pair_component_summary(
            &mut right_summary,
            component.right_contract_count,
            component.right_nft_count,
            component.left_contract_count,
            component.total_contract_count,
        );
    }
    (left_summary, right_summary)
}

fn push_metadata_chain_matrix_row(
    rows: &mut Vec<SummaryRow>,
    chains: &[String],
    totals: &HashMap<String, NameTotals>,
    primary_index: usize,
    secondary_index: usize,
    summary: GroupSummary,
) {
    let primary = &chains[primary_index];
    let total = totals.get(primary).copied().unwrap_or(NameTotals {
        contracts: 0,
        nfts: 0,
    });
    rows.push(metadata_summary_row(
        "chain_matrix",
        primary,
        &chains[secondary_index],
        total,
        summary,
    ));
}

fn metadata_summary_row(
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    total: NameTotals,
    summary: GroupSummary,
) -> SummaryRow {
    summary_row(
        SummarySpec {
            field_name: "metadata",
            scope,
            primary_chain,
            secondary_chain,
            threshold: Some(METADATA_THRESHOLD),
            match_mode: METADATA_MATCH_MODE,
            metric: "duplicate_group",
            total_contracts: total.contracts,
            total_nfts: total.nfts,
        },
        summary,
    )
}

impl MetadataBm25Document {
    fn from_text(document: &str) -> Option<Self> {
        let tokens = metadata_bm25_tokens(document);
        if tokens.is_empty() {
            return None;
        }
        let mut unique_tokens = tokens.clone();
        unique_tokens.sort_unstable();
        unique_tokens.dedup();
        Some(Self {
            tokens,
            unique_tokens,
        })
    }
}

impl InternedMetadataDoc {
    fn from_source_doc(doc: InternedMetadataSourceDoc) -> Self {
        Self {
            unique_tokens: doc.unique_tokens,
        }
    }

    fn unique_tokens(&self) -> &[usize] {
        &self.unique_tokens
    }
}

impl InternedMetadataSourceDoc {
    fn from_metadata_doc(
        doc: &MetadataBm25Document,
        token_ids: &HashMap<String, usize>,
        postings: &mut [Vec<MetadataDocIndex>],
        doc_index: usize,
    ) -> Self {
        let mut tokens = Vec::with_capacity(doc.tokens.len());
        let mut term_freqs = HashMap::new();
        for token in &doc.tokens {
            let token_id = metadata_token_id(token, token_ids);
            tokens.push(token_id);
            *term_freqs.entry(token_id).or_insert(0usize) += 1;
        }
        let mut unique_tokens = term_freqs.keys().copied().collect::<Vec<_>>();
        unique_tokens.sort_unstable();
        let compact_doc_index = metadata_doc_index_from_usize(doc_index);
        for &token_id in &unique_tokens {
            postings[token_id].push(compact_doc_index);
        }
        Self {
            tokens,
            term_freqs,
            unique_tokens,
        }
    }

    fn len(&self) -> usize {
        self.tokens.len()
    }

    fn term_frequency(&self, token: usize) -> usize {
        *self.term_freqs.get(&token).unwrap_or(&0)
    }

    fn unique_tokens(&self) -> &[usize] {
        &self.unique_tokens
    }
}

fn lexical_metadata_token_ids(entries: &[SourceMetadataDocEntry]) -> HashMap<String, usize> {
    let mut tokens = entries
        .iter()
        .flat_map(|entry| entry.doc.unique_tokens.iter().cloned())
        .collect::<Vec<_>>();
    tokens.sort_unstable();
    tokens.dedup();
    tokens
        .into_iter()
        .enumerate()
        .map(|(token_id, token)| (token, token_id))
        .collect()
}

fn metadata_token_id(token: &str, token_ids: &HashMap<String, usize>) -> usize {
    *token_ids
        .get(token)
        .expect("metadata token must be present in the lexical token id map")
}

fn metadata_contract_index_from_usize(index: usize) -> MetadataContractIndex {
    MetadataContractIndex::try_from(index)
        .expect("metadata contract count must fit in compact u32 membership indexes")
}

fn metadata_contract_index_to_usize(index: MetadataContractIndex) -> usize {
    index as usize
}

fn metadata_doc_index_from_usize(index: usize) -> MetadataDocIndex {
    MetadataDocIndex::try_from(index)
        .expect("metadata document count must fit in compact u32 postings")
}

fn metadata_doc_index_to_usize(index: MetadataDocIndex) -> usize {
    index as usize
}

impl InternedMetadataCorpus {
    fn from_doc_weights(
        doc_weights: &[usize],
        docs: &[InternedMetadataSourceDoc],
        token_count: usize,
    ) -> Self {
        let mut total_docs = 0usize;
        let mut total_terms = 0usize;
        let mut doc_freqs = vec![0; token_count];
        for (&weight, doc) in doc_weights.iter().zip(docs) {
            if weight == 0 {
                continue;
            }
            total_docs += weight;
            total_terms += doc.len() * weight;
            for &token in doc.unique_tokens() {
                doc_freqs[token] += weight;
            }
        }
        let avg_doc_len = if total_docs == 0 {
            0.0
        } else {
            total_terms as f64 / total_docs as f64
        };
        Self {
            total_docs,
            avg_doc_len,
            doc_freqs,
        }
    }
}

impl PreparedInternedMetadataQuery {
    fn new(query: &InternedMetadataSourceDoc, corpus: &InternedMetadataCorpus) -> Self {
        let terms = query_terms_from_token_ids(&query.tokens);
        let self_score = bm25_score_terms(&terms, query, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        Self { terms, denominator }
    }
}

impl PreparedInternedMetadataDoc {
    fn new(doc: &InternedMetadataSourceDoc, corpus: &InternedMetadataCorpus) -> Self {
        if doc.len() == 0 || corpus.total_docs == 0 || corpus.avg_doc_len <= 0.0 {
            return Self {
                token_weights: Vec::new(),
            };
        }

        let doc_len = doc.len() as f64;
        let norm = METADATA_BM25_K1
            * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);
        let token_weights = doc
            .unique_tokens()
            .iter()
            .filter_map(|&token| {
                let tf = doc.term_frequency(token) as f64;
                if tf == 0.0 {
                    return None;
                }
                let df = corpus.doc_freqs.get(token).copied().unwrap_or(0) as f64;
                let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
                let weight = idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm);
                Some((token, weight))
            })
            .collect();
        Self { token_weights }
    }
}

impl InternedMetadataIndex {
    fn from_source_doc_entries(
        entries: Vec<SourceMetadataDocEntry>,
    ) -> (Self, Vec<MetadataDocEntry>) {
        let token_ids = lexical_metadata_token_ids(&entries);
        let mut postings = vec![Vec::new(); token_ids.len()];
        let mut doc_weights = Vec::with_capacity(entries.len());
        let mut source_docs = Vec::with_capacity(entries.len());
        let mut doc_entries = Vec::with_capacity(entries.len());
        for (doc_index, entry) in entries.into_iter().enumerate() {
            doc_weights.push(entry.contracts.len());
            source_docs.push(InternedMetadataSourceDoc::from_metadata_doc(
                &entry.doc,
                &token_ids,
                &mut postings,
                doc_index,
            ));
            doc_entries.push(MetadataDocEntry {
                contracts: entry.contracts,
                contracts_by_chain: entry.contracts_by_chain,
            });
        }
        for indices in &mut postings {
            indices.sort_unstable();
            indices.dedup();
        }
        let corpus =
            InternedMetadataCorpus::from_doc_weights(&doc_weights, &source_docs, token_ids.len());
        let prepared_docs = source_docs
            .par_iter()
            .map(|doc| PreparedInternedMetadataDoc::new(doc, &corpus))
            .collect::<Vec<_>>();
        let queries = source_docs
            .par_iter()
            .map(|doc| PreparedInternedMetadataQuery::new(doc, &corpus))
            .collect::<Vec<_>>();
        let docs = source_docs
            .into_iter()
            .map(InternedMetadataDoc::from_source_doc)
            .collect();
        let index = Self {
            docs,
            corpus,
            queries,
            prepared_docs,
            postings,
            #[cfg(test)]
            token_ids,
        };
        (index, doc_entries)
    }

    #[cfg(test)]
    fn token_id(&self, token: &str) -> Option<usize> {
        self.token_ids.get(token).copied()
    }
}

fn score_metadata_with_prepared_doc(
    query: &PreparedInternedMetadataQuery,
    right: &PreparedInternedMetadataDoc,
) -> f64 {
    if query.terms.is_empty() || right.token_weights.is_empty() {
        return 0.0;
    }
    (bm25_score_prepared_terms(&query.terms, &right.token_weights) / query.denominator)
        .clamp(0.0, 1.0)
}

fn bm25_score_prepared_terms(
    query_terms: &[(usize, usize)],
    doc_token_weights: &[(usize, f64)],
) -> f64 {
    let mut score = 0.0;
    let mut query_index = 0usize;
    let mut doc_index = 0usize;
    while query_index < query_terms.len() && doc_index < doc_token_weights.len() {
        let (query_token, query_tf) = query_terms[query_index];
        let (doc_token, doc_weight) = doc_token_weights[doc_index];
        match query_token.cmp(&doc_token) {
            std::cmp::Ordering::Less => query_index += 1,
            std::cmp::Ordering::Greater => doc_index += 1,
            std::cmp::Ordering::Equal => {
                score += query_tf as f64 * doc_weight;
                query_index += 1;
                doc_index += 1;
            }
        }
    }
    score
}

fn query_terms_from_token_ids(query_tokens: &[usize]) -> Vec<(usize, usize)> {
    if query_tokens.is_empty() {
        return Vec::new();
    }
    let mut tokens = query_tokens.to_vec();
    tokens.sort_unstable();
    let mut terms = Vec::new();
    let mut iter = tokens.into_iter();
    let Some(mut current) = iter.next() else {
        return Vec::new();
    };
    let mut count = 1usize;
    for token in iter {
        if token == current {
            count += 1;
        } else {
            terms.push((current, count));
            current = token;
            count = 1;
        }
    }
    terms.push((current, count));
    terms
}

fn bm25_score_terms(
    query_terms: &[(usize, usize)],
    doc: &InternedMetadataSourceDoc,
    corpus: &InternedMetadataCorpus,
) -> f64 {
    if query_terms.is_empty()
        || doc.len() == 0
        || corpus.total_docs == 0
        || corpus.avg_doc_len <= 0.0
    {
        return 0.0;
    }

    let doc_len = doc.len() as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / corpus.avg_doc_len);

    query_terms
        .iter()
        .map(|(token, query_tf)| {
            let tf = doc.term_frequency(*token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = corpus.doc_freqs.get(*token).copied().unwrap_or(0) as f64;
            let idf = ((corpus.total_docs as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn metadata_document_from_json(raw: &str) -> String {
    if !metadata_is_dedup_eligible(raw) {
        return String::new();
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut parts = Vec::new();
            flatten_metadata_content_values(&value, &mut parts);
            normalize_metadata_text(&parts.join(" "))
        }
        Err(_) => normalize_metadata_text(raw),
    }
}

fn metadata_is_dedup_eligible(raw: &str) -> bool {
    let raw = raw.trim();
    !raw.is_empty()
        && raw.len() <= MAX_METADATA_BYTES_FOR_DEDUP
        && matches!(raw.chars().next(), Some('{') | Some('['))
}

fn flatten_metadata_content_values(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key_norm = normalize_metadata_text(key);
                if metadata_content_key_includes_value(&key_norm) {
                    flatten_metadata_content_values(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                flatten_metadata_content_values(item, parts);
            }
        }
        Value::String(text) if !text.trim().is_empty() => parts.push(text.trim().to_string()),
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {}
    }
}

fn metadata_content_key_includes_value(key: &str) -> bool {
    matches!(
        key,
        "description"
            | "trait_type"
            | "value"
            | "display_type"
            | "image"
            | "image_url"
            | "animation_url"
            | "external_url"
            | "attributes"
            | "metadata"
            | "rawmetadata"
            | "raw"
    )
}

fn normalize_metadata_text(raw: &str) -> String {
    raw.nfkc()
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn metadata_bm25_tokens(document: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in normalize_metadata_text(document).chars() {
        if ch.is_alphanumeric() || ch == '_' {
            current.push(ch);
        } else if !current.is_empty() {
            if current.len() >= 2 {
                tokens.push(std::mem::take(&mut current));
            } else {
                current.clear();
            }
        }
    }
    if current.len() >= 2 {
        tokens.push(current);
    }
    tokens
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    fn metadata_doc_entry(text: &str) -> SourceMetadataDocEntry {
        SourceMetadataDocEntry {
            doc: MetadataBm25Document::from_text(text).unwrap(),
            contracts: vec![0],
            contracts_by_chain: vec![vec![0]],
        }
    }

    #[test]
    fn metadata_document_key_uses_direct_document_text() {
        let left = "gold dragon gold";
        let right = "dragon gold gold";

        assert_ne!(metadata_document_key(left), metadata_document_key(right));
    }

    #[test]
    fn metadata_document_uses_top_contract_content_values_for_global_matching() {
        let document = metadata_document_from_json(
            r#"{
                "description": "Gold Dragon",
                "attributes": [
                    {"trait_type": "Background", "value": "Gold"},
                    {"trait_type": "Background", "value": "Gold"},
                    {"trait_type": "Eyes", "value": "Laser"}
                ],
                "seller_fee_basis_points": 500,
                "irrelevant": "Hidden Lore"
            }"#,
        );

        assert!(document.contains("gold dragon"));
        assert!(document.contains("background"));
        assert!(document.contains("eyes"));
        assert!(document.contains("laser"));
        assert!(!document.contains("seller_fee_basis_points"));
        assert!(!document.contains("hidden lore"));
    }

    #[test]
    fn metadata_document_preserves_content_values_for_representative_matching() {
        let left = metadata_document_from_json(
            r#"{
                "name": "Alpha #1",
                "image": "ipfs://alpha/1.png",
                "attributes": [
                    {"trait_type": "Background", "value": "Blue"}
                ]
            }"#,
        );
        let right = metadata_document_from_json(
            r#"{
                "name": "Beta #9",
                "image": "ipfs://beta/9.png",
                "attributes": [
                    {"trait_type": "Background", "value": "Red"}
                ]
            }"#,
        );

        assert!(left.contains("alpha"));
        assert!(left.contains("blue"));
        assert!(right.contains("beta"));
        assert!(right.contains("red"));
        assert_ne!(metadata_document_key(&left), metadata_document_key(&right));
    }

    #[test]
    fn metadata_document_rejects_non_json_and_overlong_raw_metadata() {
        assert_eq!(metadata_document_from_json("not json metadata"), "");
        let overlong = format!(
            "{{\"description\":\"{}\"}}",
            "x".repeat(MAX_METADATA_BYTES_FOR_DEDUP)
        );

        assert_eq!(metadata_document_from_json(&overlong), "");
    }

    #[test]
    fn metadata_document_normalizes_nfkc_content_values() {
        let document = metadata_document_from_json(
            r#"{"description":"\uFF27\uFF4F\uFF4C\uFF44\u3000Dragon"}"#,
        );

        assert_eq!(document, "gold dragon");
    }

    #[test]
    fn metadata_document_requires_informative_token_for_indexing() {
        let key_only = MetadataBm25Document::from_text("description").unwrap();
        let informative = MetadataBm25Document::from_text("description gold dragon").unwrap();

        assert!(!metadata_document_has_informative_token(&key_only));
        assert!(metadata_document_has_informative_token(&informative));
    }

    #[test]
    fn metadata_doc_pair_hits_are_collected_for_left_range() {
        let docs = vec![
            metadata_doc_entry("gold dragon alpha"),
            metadata_doc_entry("dragon gold beta"),
            metadata_doc_entry("silver cat"),
            metadata_doc_entry("gold dragon beta"),
        ];
        let (index, _) = InternedMetadataIndex::from_source_doc_entries(docs);
        let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());

        let batch = collect_metadata_doc_pair_hits_for_left_range(
            1..3,
            MetadataPairScoringContext {
                docs: &index.docs,
                corpus: &index.corpus,
                postings: &index.postings,
                queries: &index.queries,
                prepared_docs: &index.prepared_docs,
            },
            &scratch_pool,
        );

        assert_eq!(
            batch,
            MetadataDocPairBatch {
                hits: vec![(1, 3)],
                candidate_pairs: 1,
            }
        );
    }

    #[test]
    fn metadata_doc_pair_hits_score_one_left_with_reused_scratch() {
        let docs = vec![
            metadata_doc_entry("gold dragon alpha omega"),
            metadata_doc_entry("dragon gold alpha"),
            metadata_doc_entry("silver cat"),
            metadata_doc_entry("gold dragon omega"),
        ];
        let (index, _) = InternedMetadataIndex::from_source_doc_entries(docs);
        let mut scratch = MetadataCandidateScratch::new(index.docs.len());
        let mut hits = Vec::new();

        let candidate_pairs = collect_metadata_doc_pair_hits_for_left_with_scratch(
            0,
            &MetadataPairScoringContext {
                docs: &index.docs,
                corpus: &index.corpus,
                postings: &index.postings,
                queries: &index.queries,
                prepared_docs: &index.prepared_docs,
            },
            &mut scratch,
            &mut hits,
        );

        assert_eq!(candidate_pairs, 2);
        assert_eq!(hits, vec![(0, 1), (0, 3)]);
    }

    #[test]
    fn metadata_pair_progress_message_shows_throughput_and_eta() {
        assert_eq!(
            metadata_pair_progress_message(333, 2, 6, 7, std::time::Duration::from_secs(2)),
            "metadata candidate pairs scored 333; left docs 2/6; estimated remaining 666; throughput 166.5 pairs/s; ETA 4s; matched doc pairs 7"
        );
    }

    #[test]
    fn metadata_pair_progress_message_uses_unknown_eta_before_first_scored_pair() {
        assert_eq!(
            metadata_pair_progress_message(0, 0, 6, 0, std::time::Duration::from_secs(0)),
            "metadata candidate pairs scored 0; left docs 0/6; estimated remaining 0; throughput n/a; ETA n/a; matched doc pairs 0"
        );
    }

    #[test]
    fn metadata_scoring_progress_units_track_left_docs_not_candidate_pairs() {
        assert_eq!(metadata_scoring_progress_units(10), 10);
        assert_eq!(metadata_scoring_batch_progress_units(2, 7), 5);
    }

    #[test]
    fn metadata_candidate_scratch_deduplicates_posting_hits() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("dragon gold"),
            metadata_doc_entry("gold cat"),
        ];
        let (index, _) = InternedMetadataIndex::from_source_doc_entries(docs);
        let mut scratch = MetadataCandidateScratch::new(3);

        let first = metadata_candidate_indices_for_left_with_scratch(
            0,
            &index.docs[0],
            &index.postings,
            &mut scratch,
        )
        .to_vec();
        let second = metadata_candidate_indices_for_left_with_scratch(
            1,
            &index.docs[1],
            &index.postings,
            &mut scratch,
        )
        .to_vec();

        assert_eq!(first, vec![1, 2]);
        assert_eq!(second, vec![2]);
    }

    #[test]
    fn metadata_candidate_scratch_pool_reuses_returned_scratch() {
        let pool = MetadataCandidateScratchPool::new(3);
        let mut scratch = pool.take();
        scratch.clear_for_next_left();
        scratch.push_once(1);
        let original_ptr = scratch.seen_generation.as_ptr();

        pool.put(scratch);
        let reused = pool.take();

        assert_eq!(reused.seen_generation.as_ptr(), original_ptr);
    }

    #[test]
    fn metadata_bm25_index_interns_tokens_and_integer_postings() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("dragon gold"),
            metadata_doc_entry("silver cat"),
        ];
        let (index, _) = InternedMetadataIndex::from_source_doc_entries(docs);
        let gold = index.token_id("gold").unwrap();
        let dragon = index.token_id("dragon").unwrap();

        assert_eq!(index.postings[gold], vec![0, 1]);
        assert_eq!(index.postings[dragon], vec![0, 1]);
        let _: &[u32] = index.postings[gold].as_slice();
        assert!(index.docs[0].unique_tokens().contains(&gold));
        assert!(index.queries[0].terms.iter().any(|(token, tf)| {
            *token == gold && *tf == 1
        }));
    }

    #[test]
    fn metadata_bm25_index_assigns_lexical_token_ids_for_stable_score_order() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("silver cat"),
        ];
        let (index, _) = InternedMetadataIndex::from_source_doc_entries(docs);

        assert!(
            index.token_id("cat").unwrap()
                < index.token_id("dragon").unwrap()
                && index.token_id("dragon").unwrap() < index.token_id("gold").unwrap()
                && index.token_id("gold").unwrap() < index.token_id("silver").unwrap()
        );
    }

    #[test]
    fn prepared_metadata_doc_score_matches_bm25_terms() {
        let docs = vec![
            metadata_doc_entry("gold dragon gold rare"),
            metadata_doc_entry("gold dragon rare shiny"),
        ];
        let token_ids = lexical_metadata_token_ids(&docs);
        let mut postings = vec![Vec::new(); token_ids.len()];
        let left = InternedMetadataSourceDoc::from_metadata_doc(
            &docs[0].doc,
            &token_ids,
            &mut postings,
            0,
        );
        let right = InternedMetadataSourceDoc::from_metadata_doc(
            &docs[1].doc,
            &token_ids,
            &mut postings,
            1,
        );
        let source_docs = vec![left, right];
        let corpus = InternedMetadataCorpus::from_doc_weights(&[1, 1], &source_docs, token_ids.len());
        let terms = query_terms_from_token_ids(&source_docs[0].tokens);
        let denominator = bm25_score_terms(&terms, &source_docs[0], &corpus);
        let expected = (bm25_score_terms(&terms, &source_docs[1], &corpus) / denominator)
            .clamp(0.0, 1.0);

        let (index, _) = InternedMetadataIndex::from_source_doc_entries(docs);

        let actual = score_metadata_with_prepared_doc(&index.queries[0], &index.prepared_docs[1]);

        assert!((actual - expected).abs() < 1e-12);
    }

    #[test]
    fn metadata_data_builder_builds_bm25_index_for_content_representative_matching() {
        let mut builder = MetadataDataBuilder::new(1);
        let doc = MetadataBm25Document::from_text("gold dragon").unwrap();
        let doc_key = metadata_document_key("gold dragon");
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            contract_address: "0xaaa".to_string(),
            nft_count: 2,
            doc,
            doc_key,
        });

        let data = builder.finish();

        assert_eq!(data.docs[0].contracts, vec![0]);
        assert_eq!(data.metadata_index.docs.len(), 1);
        assert!(data.metadata_index.token_id("gold").is_some());
    }

    #[test]
    fn metadata_memberships_use_compact_contract_indexes() {
        let mut builder = MetadataDataBuilder::new(1);
        let doc = MetadataBm25Document::from_text("gold dragon").unwrap();
        let doc_key = metadata_document_key("gold dragon");
        builder.merge_indexed_row(IndexedMetadataRow {
            chain_index: 0,
            contract_address: "0xaaa".to_string(),
            nft_count: 2,
            doc,
            doc_key,
        });

        let data = builder.finish();

        let _: &[MetadataContractIndex] = data.docs[0].contracts.as_slice();
        let _: &[MetadataContractIndex] = data.docs[0].contracts_by_chain[0].as_slice();
        let _: &[MetadataContractIndex] = data.contracts_by_chain[0].as_slice();
    }

    #[test]
    fn metadata_index_consumes_source_docs_and_returns_lightweight_entries() {
        let docs = vec![metadata_doc_entry("gold dragon")];

        let (index, entries) = InternedMetadataIndex::from_source_doc_entries(docs);

        assert_eq!(entries[0].contracts, vec![0]);
        assert_eq!(index.docs.len(), 1);
        assert!(index.token_id("gold").is_some());
    }

    #[test]
    fn interned_metadata_index_keeps_only_compact_candidate_docs_after_preparation() {
        let docs = vec![metadata_doc_entry("gold dragon gold")];

        let (index, _) = InternedMetadataIndex::from_source_doc_entries(docs);

        assert_eq!(
            std::mem::size_of::<InternedMetadataDoc>(),
            std::mem::size_of::<Vec<usize>>()
        );
        assert_eq!(index.docs[0].unique_tokens().len(), 2);
    }

    #[test]
    fn metadata_raw_row_chunk_indexes_valid_rows_only() {
        let chains = ["ethereum".to_string(), "base".to_string()];
        let chain_indexes = chains
            .iter()
            .enumerate()
            .map(|(index, chain)| (chain.as_str(), index))
            .collect::<HashMap<_, _>>();
        let rows = vec![
            RawMetadataRow {
                chain: "ethereum".into(),
                contract_address: "0xaaa".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                nft_count: 2,
            },
            RawMetadataRow {
                chain: "missing".into(),
                contract_address: "0xbbb".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
                nft_count: 1,
            },
            RawMetadataRow {
                chain: "base".into(),
                contract_address: "0xccc".into(),
                metadata_json: "not json".into(),
                nft_count: 1,
            },
        ];

        let indexed = index_metadata_raw_row_chunk(rows, &chain_indexes);

        assert_eq!(indexed.len(), 1);
        assert_eq!(indexed[0].chain_index, 0);
        assert_eq!(indexed[0].contract_address, "0xaaa");
        assert_eq!(indexed[0].nft_count, 2);
    }

}
