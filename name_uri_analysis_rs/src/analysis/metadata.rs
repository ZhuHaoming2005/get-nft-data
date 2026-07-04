use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

const METADATA_THRESHOLD: f64 = 0.6;
const METADATA_MATCH_MODE: &str = "template_recall_hybrid_verify";
const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;
const METADATA_BM25_K1: f64 = 1.2;
const METADATA_BM25_B: f64 = 0.75;
const METADATA_LOAD_CHUNK_ROWS: usize = 16 * 1024;
const METADATA_PAIR_LEFT_CHUNK_SIZE: usize = 256;
const METADATA_CONTENT_PARALLEL_MIN_RECORDS: usize = 64;
const METADATA_CONTENT_SCORE_BATCH_PAIRS: usize = 16 * 1024;
const METADATA_SKETCH_ANCHOR_COUNT: usize = 16;
const METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD: u32 = 32;
const METADATA_SKETCH_HIGH_FREQ_MIN_DOCS: usize = 32;
const METADATA_SKETCH_HIGH_FREQ_DIVISOR: usize = 5;
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
    content_document: String,
    doc: MetadataBm25Document,
    doc_key: MetadataDocKey,
}

#[derive(Clone, Debug)]
struct MetadataContract {
    chain_index: usize,
    contract_address: String,
    nft_count: i64,
    content_doc: Option<MetadataBm25Document>,
    template_doc_index: MetadataDocIndex,
}

#[derive(Debug)]
struct SourceMetadataDocEntry {
    doc: MetadataBm25Document,
    contracts: Vec<MetadataContractIndex>,
}

#[derive(Debug)]
struct MetadataData {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    metadata_index: InternedMetadataIndex,
}

#[derive(Debug, Default)]
struct MetadataTemplateMatches {
    compatible_docs: HashMap<MetadataDocIndex, Vec<MetadataDocIndex>>,
}

struct MetadataDataBuilder {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<MetadataContractIndex>>,
    contract_index_by_key: HashMap<(usize, String), usize>,
    docs: Vec<SourceMetadataDocEntry>,
    doc_index_by_key: HashMap<MetadataDocKey, usize>,
}

#[derive(Debug, Clone)]
struct MetadataBm25Document {
    tokens: Vec<String>,
    unique_tokens: Vec<String>,
    term_freqs: HashMap<String, usize>,
}

#[derive(Debug)]
struct MetadataContentRecord {
    contract_index: MetadataContractIndex,
    doc: MetadataBm25Document,
}

#[derive(Debug)]
struct MetadataContentAtom {
    chain_index: usize,
    template_doc_index: MetadataDocIndex,
    representative_record_index: MetadataDocIndex,
    members: Vec<MetadataContractIndex>,
    fallback_token_groups: Vec<MetadataFallbackTokenGroup>,
}

#[derive(Debug)]
struct MetadataFallbackTokenGroup {
    members: Vec<MetadataContractIndex>,
}

struct MetadataContentCandidateIndex<'a> {
    postings: HashMap<(&'a str, MetadataDocIndex), Vec<MetadataDocIndex>>,
}

#[derive(Clone, Copy, Debug)]
enum MetadataContentScope {
    SharedToken,
    NoCommonToken,
}

#[derive(Clone, Debug, Default)]
struct MetadataSketch {
    simhash: u64,
    anchors: Vec<usize>,
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
    candidate_tokens: Vec<usize>,
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
    sketches: Vec<MetadataSketch>,
    #[cfg(test)]
    token_ids: HashMap<String, usize>,
    #[cfg(test)]
    build_thread_count: usize,
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
    sketches: &'a [MetadataSketch],
    postings: &'a [Vec<MetadataDocIndex>],
    queries: &'a [PreparedInternedMetadataQuery],
    prepared_docs: &'a [PreparedInternedMetadataDoc],
}

struct MetadataContentUnionContext<'a> {
    data: &'a MetadataData,
    template_matches: &'a MetadataTemplateMatches,
    contract_tokens: &'a [Vec<u32>],
    chain_count: usize,
    pool: &'a rayon::ThreadPool,
}

struct MetadataUnionState {
    intra: UnionFind,
    cross: Option<SparseUnionFind>,
    chain_matrix: Option<Vec<SparseUnionFind>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct MetadataContentUnionStats {
    atom_count: usize,
    candidate_pairs: u64,
    scored_pairs: u64,
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

impl MetadataTemplateMatches {
    fn from_pairs(pairs: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut compatible_docs =
            HashMap::<MetadataDocIndex, Vec<MetadataDocIndex>>::new();
        for (left, right) in pairs {
            let left = metadata_doc_index_from_usize(left);
            let right = metadata_doc_index_from_usize(right);
            if left != right {
                compatible_docs.entry(left).or_default().push(right);
                compatible_docs.entry(right).or_default().push(left);
            }
        }
        for docs in compatible_docs.values_mut() {
            docs.sort_unstable();
            docs.dedup();
        }
        Self { compatible_docs }
    }

    fn matches(&self, left: usize, right: usize) -> bool {
        left == right
            || self
                .compatible_docs(metadata_doc_index_from_usize(left))
                .binary_search(&metadata_doc_index_from_usize(right))
                .is_ok()
    }

    fn compatible_docs(&self, doc: MetadataDocIndex) -> &[MetadataDocIndex] {
        self.compatible_docs.get(&doc).map_or(&[], Vec::as_slice)
    }
}

impl<'a> MetadataContentCandidateIndex<'a> {
    #[cfg(test)]
    fn new(
        records: &'a [MetadataContentRecord],
        template_docs: &[MetadataDocIndex],
    ) -> Self {
        debug_assert_eq!(records.len(), template_docs.len());
        let mut postings = HashMap::new();
        for (record_index, (record, &template_doc)) in
            records.iter().zip(template_docs).enumerate()
        {
            let record_index = metadata_doc_index_from_usize(record_index);
            for token in &record.doc.unique_tokens {
                postings
                    .entry((token.as_str(), template_doc))
                    .or_insert_with(Vec::new)
                    .push(record_index);
            }
        }
        Self { postings }
    }

    fn from_atoms(
        records: &'a [MetadataContentRecord],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut postings = HashMap::new();
        for (atom_index, atom) in atoms.iter().enumerate() {
            let compact_atom_index = metadata_doc_index_from_usize(atom_index);
            let record =
                &records[metadata_doc_index_to_usize(atom.representative_record_index)];
            for token in &record.doc.unique_tokens {
                postings
                    .entry((token.as_str(), atom.template_doc_index))
                    .or_insert_with(Vec::new)
                    .push(compact_atom_index);
            }
        }
        Self { postings }
    }

    fn from_atoms_parallel(
        records: &'a [MetadataContentRecord],
        atoms: &[MetadataContentAtom],
    ) -> Self {
        let mut postings = (0..atoms.len())
            .into_par_iter()
            .fold(HashMap::new, |mut local, atom_index| {
                let atom = &atoms[atom_index];
                let record = &records[metadata_doc_index_to_usize(
                    atom.representative_record_index,
                )];
                let compact_atom_index =
                    metadata_doc_index_from_usize(atom_index);
                for token in &record.doc.unique_tokens {
                    local
                        .entry((token.as_str(), atom.template_doc_index))
                        .or_insert_with(Vec::new)
                        .push(compact_atom_index);
                }
                local
            })
            .reduce(HashMap::new, |mut left, mut right| {
                if left.len() < right.len() {
                    std::mem::swap(&mut left, &mut right);
                }
                for (key, mut posting) in right {
                    left.entry(key)
                        .or_insert_with(Vec::new)
                        .append(&mut posting);
                }
                left
            });
        postings
            .par_iter_mut()
            .for_each(|(_, posting)| posting.sort_unstable());
        Self { postings }
    }

    fn append_candidates_after(
        &self,
        record_index: usize,
        record: &MetadataContentRecord,
        template_doc: MetadataDocIndex,
        template_matches: &MetadataTemplateMatches,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let compact_record_index = metadata_doc_index_from_usize(record_index);
        for token in &record.doc.unique_tokens {
            self.append_posting_after(
                token,
                template_doc,
                compact_record_index,
                scratch,
            );
            for &compatible_doc in template_matches.compatible_docs(template_doc) {
                self.append_posting_after(
                    token,
                    compatible_doc,
                    compact_record_index,
                    scratch,
                );
            }
        }
    }

    fn append_posting_after(
        &self,
        token: &str,
        template_doc: MetadataDocIndex,
        record_index: MetadataDocIndex,
        scratch: &mut MetadataCandidateScratch,
    ) {
        let Some(posting) = self.postings.get(&(token, template_doc)) else {
            return;
        };
        let start = posting.partition_point(|&right| right <= record_index);
        for &right in &posting[start..] {
            scratch.push_once(right);
        }
    }
}

fn sorted_metadata_anchors_intersect(left: &[usize], right: &[usize]) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

fn metadata_sketch_source_match(
    left: &MetadataSketch,
    right: &MetadataSketch,
    hamming_threshold: u32,
) -> bool {
    if (left.simhash == 0 && left.anchors.is_empty())
        || (right.simhash == 0 && right.anchors.is_empty())
    {
        return false;
    }
    if !left.anchors.is_empty()
        && sorted_metadata_anchors_intersect(&left.anchors, &right.anchors)
    {
        return true;
    }
    (left.simhash ^ right.simhash).count_ones() <= hamming_threshold
}

fn stable_metadata_token_hash(token: &str) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325u64;
    for byte in token.as_bytes() {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn metadata_token_idf(total_docs: usize, doc_freq: usize) -> f64 {
    (((total_docs + 1) as f64) / ((doc_freq + 1) as f64)).ln() + 1.0
}

fn metadata_token_is_high_frequency(total_docs: usize, doc_freq: usize) -> bool {
    doc_freq >= METADATA_SKETCH_HIGH_FREQ_MIN_DOCS
        && doc_freq.saturating_mul(METADATA_SKETCH_HIGH_FREQ_DIVISOR) > total_docs
}

fn metadata_sketch_from_interned_document(
    document: &InternedMetadataSourceDoc,
    corpus: &InternedMetadataCorpus,
    token_hashes: &[u64],
) -> MetadataSketch {
    let mut weights = [0.0f64; 64];
    let mut anchor_candidates = Vec::new();
    for &token in document.unique_tokens() {
        let doc_freq = corpus.doc_freqs.get(token).copied().unwrap_or(0);
        let idf = metadata_token_idf(corpus.total_docs, doc_freq);
        let token_hash = token_hashes.get(token).copied().unwrap_or(0);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if ((token_hash >> bit) & 1) == 1 {
                *weight += idf;
            } else {
                *weight -= idf;
            }
        }
        if !metadata_token_is_high_frequency(corpus.total_docs, doc_freq) {
            anchor_candidates.push((token, doc_freq));
        }
    }
    anchor_candidates.sort_unstable_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut anchors = anchor_candidates
        .into_iter()
        .take(METADATA_SKETCH_ANCHOR_COUNT)
        .map(|(token, _)| token)
        .collect::<Vec<_>>();
    anchors.sort_unstable();
    let mut simhash = 0u64;
    for (bit, weight) in weights.into_iter().enumerate() {
        if weight >= 0.0 {
            simhash |= 1u64 << bit;
        }
    }
    MetadataSketch { simhash, anchors }
}

impl MetadataDataBuilder {
    fn new(chain_count: usize) -> Self {
        Self {
            contracts: Vec::new(),
            contracts_by_chain: vec![Vec::new(); chain_count],
            contract_index_by_key: HashMap::new(),
            docs: Vec::new(),
            doc_index_by_key: HashMap::new(),
        }
    }

    fn merge_indexed_rows(&mut self, indexed_rows: Vec<IndexedMetadataRow>) {
        for row in indexed_rows {
            self.merge_indexed_row(row);
        }
    }

    fn merge_indexed_row(&mut self, row: IndexedMetadataRow) {
        let doc_index = match self.doc_index_by_key.get(&row.doc_key).copied() {
            Some(index) => index,
            None => {
                let index = self.docs.len();
                self.doc_index_by_key.insert(row.doc_key, index);
                self.docs.push(SourceMetadataDocEntry {
                    doc: row.doc,
                    contracts: Vec::new(),
                });
                index
            }
        };
        let compact_doc_index = metadata_doc_index_from_usize(doc_index);
        let contract_key = (row.chain_index, row.contract_address.clone());
        let contract_index = match self.contract_index_by_key.get(&contract_key).copied() {
            Some(index) => {
                debug_assert_eq!(
                    self.contracts[index].template_doc_index,
                    compact_doc_index
                );
                index
            }
            None => {
                let index = self.contracts.len();
                self.contract_index_by_key.insert(contract_key, index);
                self.contracts.push(MetadataContract {
                    chain_index: row.chain_index,
                    contract_address: row.contract_address,
                    nft_count: row.nft_count,
                    content_doc: MetadataBm25Document::from_text(&row.content_document),
                    template_doc_index: compact_doc_index,
                });
                self.contracts_by_chain[row.chain_index]
                    .push(metadata_contract_index_from_usize(index));
                index
            }
        };

        let compact_contract_index = metadata_contract_index_from_usize(contract_index);
        self.docs[doc_index].contracts.push(compact_contract_index);
    }

    fn finish(self) -> MetadataData {
        let metadata_index = InternedMetadataIndex::from_source_doc_entries(self.docs);
        MetadataData {
            contracts: self.contracts,
            contracts_by_chain: self.contracts_by_chain,
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
        data.metadata_index.docs.len(),
        data.contracts.len()
    ));
    let mut rows = Vec::new();
    if data.contracts.len() < 2 || data.metadata_index.docs.is_empty() {
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
    let template_matches =
        pool.install(|| collect_metadata_template_matches(&data, progress));
    prepare_metadata_contract_token_rows(conn, &data, chains)?;
    let contract_tokens = load_metadata_contract_tokens(conn, data.contracts.len())?;
    let content_context = MetadataContentUnionContext {
        data: &data,
        template_matches: &template_matches,
        contract_tokens: &contract_tokens,
        chain_count: chains.len(),
        pool: &pool,
    };
    union_metadata_token_content_matches(conn, &content_context, &mut state)?;
    union_metadata_representative_content_fallback(
        &content_context,
        &mut state,
    );
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

    Ok(pool.install(|| builder.finish()))
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
                       token_id,
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
            ranked_metadata AS (
                SELECT chain,
                       contract_address,
                       metadata_row_id,
                       metadata_json,
                       row_number() OVER (
                           PARTITION BY chain, contract_address
                           ORDER BY token_id, metadata_row_id
                       ) AS metadata_rank,
                       count(*) OVER (
                           PARTITION BY chain, contract_address
                       )::BIGINT AS metadata_count
                FROM eligible_metadata
            )
            SELECT m.chain,
                   m.contract_address,
                   m.metadata_json,
                   t.nft_count,
                   m.metadata_count
            FROM ranked_metadata m
            JOIN totals t
              ON t.chain = m.chain
             AND t.contract_address = m.contract_address
            WHERE m.metadata_rank = 1
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
            if !metadata_is_dedup_eligible(&row.metadata_json) {
                return None;
            }
            let prefilter_document =
                metadata_prefilter_document_from_json(&row.metadata_json);
            let content_document = metadata_document_from_json(&row.metadata_json);
            let doc = MetadataBm25Document::from_text(&prefilter_document)?;
            let doc_key = metadata_document_key(&prefilter_document);
            Some(IndexedMetadataRow {
                chain_index,
                contract_address: row.contract_address,
                nft_count: row.nft_count,
                content_document,
                doc,
                doc_key,
            })
        })
        .collect()
}

fn metadata_document_key(document: &str) -> MetadataDocKey {
    document.to_string()
}

fn collect_metadata_template_matches(
    data: &MetadataData,
    progress: &ProgressTracker,
) -> MetadataTemplateMatches {
    let index = &data.metadata_index;
    if index.corpus.total_docs == 0 {
        return MetadataTemplateMatches::default();
    }
    let scoring_left_count = index.docs.len();
    let mut scored_candidate_pairs = 0u64;
    let mut scored_left_docs = 0usize;
    let mut matched_doc_pairs = 0u64;
    let mut matched_docs = Vec::new();
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
                sketches: &index.sketches,
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
        matched_docs.extend(batch.hits);
    }
    matched_docs.sort_unstable();
    matched_docs.dedup();
    MetadataTemplateMatches::from_pairs(matched_docs)
}

fn lowest_common_metadata_token(left: &[u32], right: &[u32]) -> Option<u32> {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Equal => return Some(left[left_index]),
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    None
}

fn prepare_metadata_contract_token_rows(
    conn: &Connection,
    data: &MetadataData,
    chains: &[String],
) -> Result<(), AnalysisError> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS metadata_contract_lookup;
        DROP TABLE IF EXISTS metadata_contract_token_rows;
        CREATE TEMP TABLE metadata_contract_lookup (
            contract_index BIGINT,
            chain VARCHAR,
            contract_address VARCHAR
        );
        ",
    )?;
    let mut appender = conn.appender("metadata_contract_lookup")?;
    for (contract_index, contract) in data.contracts.iter().enumerate() {
        appender.append_row(params![
            contract_index as i64,
            &chains[contract.chain_index],
            &contract.contract_address
        ])?;
    }
    appender.flush()?;
    drop(appender);
    conn.execute_batch(&format!(
        "
        CREATE TEMP TABLE metadata_contract_token_rows AS
        WITH unique_metadata AS (
            SELECT l.contract_index,
                   a.token_id,
                   min(a.rowid)::BIGINT AS metadata_row_id
            FROM analysis_rows a
            JOIN metadata_contract_lookup l
              ON l.chain = a.chain
             AND l.contract_address = a.contract_address
            WHERE a.token_id <> ''
              AND a.metadata_json <> ''
              AND length(a.metadata_json) <= {MAX_METADATA_BYTES_FOR_DEDUP}
              AND (
                  starts_with(a.metadata_json, '{{')
                  OR starts_with(a.metadata_json, '[')
              )
            GROUP BY l.contract_index, a.token_id
        )
        SELECT contract_index,
               (dense_rank() OVER (ORDER BY token_id) - 1)::BIGINT AS token_index,
               metadata_row_id
        FROM unique_metadata;
        "
    ))?;
    Ok(())
}

fn load_metadata_contract_tokens(
    conn: &Connection,
    contract_count: usize,
) -> Result<Vec<Vec<u32>>, AnalysisError> {
    let mut contract_tokens = vec![Vec::new(); contract_count];
    let mut stmt = conn.prepare(
        "
        SELECT contract_index, token_index
        FROM metadata_contract_token_rows
        ORDER BY contract_index, token_index
        ",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (contract_index, token_index) = row?;
        let contract_index = usize::try_from(contract_index).map_err(|_| {
            AnalysisError::InvalidData("negative metadata contract index".to_string())
        })?;
        let token_index = u32::try_from(token_index).map_err(|_| {
            AnalysisError::InvalidData(
                "metadata token dictionary exceeds compact u32 indexes".to_string(),
            )
        })?;
        let tokens = contract_tokens.get_mut(contract_index).ok_or_else(|| {
            AnalysisError::InvalidData(format!(
                "metadata contract index {contract_index} exceeds loaded contract count"
            ))
        })?;
        tokens.push(token_index);
    }
    Ok(contract_tokens)
}

fn union_metadata_token_content_matches(
    conn: &Connection,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> Result<(), AnalysisError> {
    let mut stmt = conn.prepare(
        "
        WITH shared_tokens AS (
            SELECT token_index
            FROM metadata_contract_token_rows
            GROUP BY token_index
            HAVING count(*) >= 2
        )
        SELECT t.token_index, t.contract_index, a.metadata_json
        FROM metadata_contract_token_rows t
        JOIN shared_tokens s ON s.token_index = t.token_index
        JOIN analysis_rows a ON a.rowid = t.metadata_row_id
        ORDER BY t.token_index, t.contract_index
        ",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut current_token = None;
    let mut raw_records = Vec::new();
    for row in rows {
        let (token_index, contract_index, metadata_json) = row?;
        let token_index = u32::try_from(token_index).map_err(|_| {
            AnalysisError::InvalidData(
                "metadata token dictionary exceeds compact u32 indexes".to_string(),
            )
        })?;
        let contract_index = MetadataContractIndex::try_from(contract_index).map_err(|_| {
            AnalysisError::InvalidData(
                "metadata contract index exceeds compact u32 indexes".to_string(),
            )
        })?;
        if current_token.is_some_and(|current| current != token_index) {
            union_metadata_raw_token_group(
                std::mem::take(&mut raw_records),
                context,
                state,
            );
        }
        current_token = Some(token_index);
        raw_records.push((contract_index, metadata_json));
    }
    if current_token.is_some() {
        union_metadata_raw_token_group(raw_records, context, state);
    }
    Ok(())
}

fn union_metadata_raw_token_group(
    raw_records: Vec<(MetadataContractIndex, String)>,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    if raw_records.len() < 2 {
        return;
    }
    let records = if raw_records.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        context.pool.install(|| {
            raw_records
                .into_par_iter()
                .filter_map(|(contract_index, metadata_json)| {
                    MetadataBm25Document::from_text(
                        &metadata_document_from_json(&metadata_json),
                    )
                    .map(|doc| MetadataContentRecord {
                        contract_index,
                        doc,
                    })
                })
                .collect::<Vec<_>>()
        })
    } else {
        raw_records
            .into_iter()
            .filter_map(|(contract_index, metadata_json)| {
                MetadataBm25Document::from_text(&metadata_document_from_json(
                    &metadata_json,
                ))
                .map(|doc| MetadataContentRecord {
                        contract_index,
                        doc,
                    })
            })
            .collect::<Vec<_>>()
    };
    union_metadata_content_candidates(
        &records,
        MetadataContentScope::SharedToken,
        context,
        state,
    );
}

fn union_metadata_representative_content_fallback(
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    let records = if context.data.contracts.len()
        >= METADATA_CONTENT_PARALLEL_MIN_RECORDS
    {
        context.pool.install(|| {
            context
                .data
                .contracts
                .par_iter()
                .enumerate()
                .filter_map(|(contract_index, contract)| {
                    contract.content_doc.clone().map(|doc| {
                        MetadataContentRecord {
                            contract_index: metadata_contract_index_from_usize(
                                contract_index,
                            ),
                            doc,
                        }
                    })
                })
                .collect::<Vec<_>>()
        })
    } else {
        context
            .data
            .contracts
            .iter()
            .enumerate()
            .filter_map(|(contract_index, contract)| {
                contract
                    .content_doc
                    .clone()
                    .map(|doc| MetadataContentRecord {
                        contract_index: metadata_contract_index_from_usize(
                            contract_index,
                        ),
                        doc,
                    })
            })
            .collect::<Vec<_>>()
    };
    union_metadata_content_candidates(
        &records,
        MetadataContentScope::NoCommonToken,
        context,
        state,
    );
}

fn apply_metadata_contract_pair_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left: usize,
    right: usize,
) {
    let left_chain = data.contracts[left].chain_index;
    let right_chain = data.contracts[right].chain_index;
    if left_chain == right_chain {
        state.intra.union(left, right);
        return;
    }
    if let Some(cross) = &mut state.cross {
        cross.union(left, right);
    }
    if let Some(matrix) = &mut state.chain_matrix {
        let (primary_chain, secondary_chain) = if left_chain < right_chain {
            (left_chain, right_chain)
        } else {
            (right_chain, left_chain)
        };
        let pair_index = chain_pair_index(primary_chain, secondary_chain, chain_count);
        matrix[pair_index].union(left, right);
    }
}

fn apply_metadata_complete_match_group_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    members: &[MetadataContractIndex],
) {
    if members.len() < 2 {
        return;
    }
    let mut members_by_chain = vec![Vec::<usize>::new(); chain_count];
    for &member in members {
        let member = metadata_contract_index_to_usize(member);
        members_by_chain[data.contracts[member].chain_index].push(member);
    }
    for chain_members in &members_by_chain {
        let Some((&anchor, rest)) = chain_members.split_first() else {
            continue;
        };
        for &member in rest {
            apply_metadata_contract_pair_union(
                data,
                chain_count,
                state,
                anchor,
                member,
            );
        }
    }
    for left_chain in 0..chain_count {
        let Some((&left_anchor, left_rest)) =
            members_by_chain[left_chain].split_first()
        else {
            continue;
        };
        for right_members in members_by_chain.iter().skip(left_chain + 1) {
            let Some((&right_anchor, right_rest)) = right_members.split_first() else {
                continue;
            };
            apply_metadata_contract_pair_union(
                data,
                chain_count,
                state,
                left_anchor,
                right_anchor,
            );
            for &right in right_rest {
                apply_metadata_contract_pair_union(
                    data,
                    chain_count,
                    state,
                    left_anchor,
                    right,
                );
            }
            for &left in left_rest {
                apply_metadata_contract_pair_union(
                    data,
                    chain_count,
                    state,
                    left,
                    right_anchor,
                );
            }
        }
    }
}

fn apply_metadata_complete_bipartite_group_union(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left_members: &[MetadataContractIndex],
    right_members: &[MetadataContractIndex],
) {
    let Some((&left_anchor, left_rest)) = left_members.split_first() else {
        return;
    };
    let Some((&right_anchor, right_rest)) = right_members.split_first() else {
        return;
    };
    apply_metadata_contract_pair_union(
        data,
        chain_count,
        state,
        metadata_contract_index_to_usize(left_anchor),
        metadata_contract_index_to_usize(right_anchor),
    );
    for &left in left_rest {
        apply_metadata_contract_pair_union(
            data,
            chain_count,
            state,
            metadata_contract_index_to_usize(left),
            metadata_contract_index_to_usize(right_anchor),
        );
    }
    for &right in right_rest {
        apply_metadata_contract_pair_union(
            data,
            chain_count,
            state,
            metadata_contract_index_to_usize(left_anchor),
            metadata_contract_index_to_usize(right),
        );
    }
}

fn metadata_fallback_token_group_tokens<'a>(
    group: &MetadataFallbackTokenGroup,
    contract_tokens: &'a [Vec<u32>],
) -> &'a [u32] {
    let representative =
        metadata_contract_index_to_usize(group.members[0]);
    &contract_tokens[representative]
}

fn metadata_fallback_token_groups_are_disjoint(
    left: &MetadataFallbackTokenGroup,
    right: &MetadataFallbackTokenGroup,
    contract_tokens: &[Vec<u32>],
) -> bool {
    lowest_common_metadata_token(
        metadata_fallback_token_group_tokens(left, contract_tokens),
        metadata_fallback_token_group_tokens(right, contract_tokens),
    )
    .is_none()
}

fn apply_metadata_fallback_atom_internal_unions(
    atom: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    for group in &atom.fallback_token_groups {
        if metadata_fallback_token_group_tokens(group, context.contract_tokens)
            .is_empty()
        {
            apply_metadata_complete_match_group_union(
                context.data,
                context.chain_count,
                state,
                &group.members,
            );
        }
    }

    let mut unvisited =
        (0..atom.fallback_token_groups.len()).collect::<Vec<_>>();
    while let Some(root) = unvisited.pop() {
        let mut queue = vec![root];
        while let Some(current) = queue.pop() {
            let mut index = 0;
            while index < unvisited.len() {
                let other = unvisited[index];
                if !metadata_fallback_token_groups_are_disjoint(
                    &atom.fallback_token_groups[current],
                    &atom.fallback_token_groups[other],
                    context.contract_tokens,
                ) {
                    index += 1;
                    continue;
                }
                let other = unvisited.swap_remove(index);
                apply_metadata_complete_bipartite_group_union(
                    context.data,
                    context.chain_count,
                    state,
                    &atom.fallback_token_groups[current].members,
                    &atom.fallback_token_groups[other].members,
                );
                queue.push(other);
            }
        }
    }
}

fn metadata_fallback_atoms_have_disjoint_token_groups(
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
    contract_tokens: &[Vec<u32>],
) -> bool {
    left.fallback_token_groups.iter().any(|left_group| {
        right.fallback_token_groups.iter().any(|right_group| {
            metadata_fallback_token_groups_are_disjoint(
                left_group,
                right_group,
                contract_tokens,
            )
        })
    })
}

fn apply_metadata_fallback_atom_pair_union(
    left: &MetadataContentAtom,
    right: &MetadataContentAtom,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) {
    let mut unvisited_left =
        (0..left.fallback_token_groups.len()).collect::<Vec<_>>();
    let mut unvisited_right =
        (0..right.fallback_token_groups.len()).collect::<Vec<_>>();
    while let Some(root) = unvisited_left.pop() {
        let mut queue = vec![(true, root)];
        while let Some((is_left, current)) = queue.pop() {
            let (current_group, opposite_groups, unvisited_opposite) = if is_left {
                (
                    &left.fallback_token_groups[current],
                    &right.fallback_token_groups,
                    &mut unvisited_right,
                )
            } else {
                (
                    &right.fallback_token_groups[current],
                    &left.fallback_token_groups,
                    &mut unvisited_left,
                )
            };
            let mut index = 0;
            while index < unvisited_opposite.len() {
                let other = unvisited_opposite[index];
                let other_group = &opposite_groups[other];
                if !metadata_fallback_token_groups_are_disjoint(
                    current_group,
                    other_group,
                    context.contract_tokens,
                ) {
                    index += 1;
                    continue;
                }
                let other = unvisited_opposite.swap_remove(index);
                let (left_group, right_group) = if is_left {
                    (
                        current_group,
                        &right.fallback_token_groups[other],
                    )
                } else {
                    (&left.fallback_token_groups[other], current_group)
                };
                apply_metadata_complete_bipartite_group_union(
                    context.data,
                    context.chain_count,
                    state,
                    &left_group.members,
                    &right_group.members,
                );
                queue.push((!is_left, other));
            }
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
    let candidates =
        metadata_candidate_indices_for_left_with_scratch(left, context, scratch);
    let mut scored_candidates = 0u64;
    for &right in candidates {
        let right = metadata_doc_index_to_usize(right);
        if !interned_metadata_docs_share_token(&context.docs[left], &context.docs[right])
            || !metadata_sketch_source_match(
                &context.sketches[left],
                &context.sketches[right],
                METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
            )
        {
            continue;
        }
        scored_candidates = scored_candidates.saturating_add(1);
        if score_metadata_with_prepared_doc(&context.queries[left], &context.prepared_docs[right])
            >= METADATA_THRESHOLD
        {
            hits.push(ordered_metadata_doc_pair(left, right));
        }
    }
    scored_candidates
}

fn metadata_candidate_indices_for_left_with_scratch<'a>(
    left: usize,
    context: &MetadataPairScoringContext<'_>,
    scratch: &'a mut MetadataCandidateScratch,
) -> &'a [MetadataDocIndex] {
    scratch.clear_for_next_left();
    let compact_left = metadata_doc_index_from_usize(left);
    for &token in &context.queries[left].candidate_tokens {
        append_metadata_posting_except(&context.postings[token], compact_left, scratch);
    }
    scratch.candidates.sort_unstable();
    &scratch.candidates
}

fn append_metadata_posting_except(
    posting: &[MetadataDocIndex],
    excluded: MetadataDocIndex,
    scratch: &mut MetadataCandidateScratch,
) {
    for &index in posting {
        if index != excluded {
            scratch.push_once(index);
        }
    }
}

fn ordered_metadata_doc_pair(left: usize, right: usize) -> (usize, usize) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn interned_metadata_docs_share_token(
    left: &InternedMetadataDoc,
    right: &InternedMetadataDoc,
) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.unique_tokens.len() && right_index < right.unique_tokens.len() {
        match left.unique_tokens[left_index].cmp(&right.unique_tokens[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
}

fn metadata_content_pair_matches(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
    threshold: f64,
) -> bool {
    metadata_content_pair_score(left, right) >= threshold
}

fn build_metadata_content_atoms(
    records: &[MetadataContentRecord],
    data: &MetadataData,
) -> Vec<MetadataContentAtom> {
    let mut atom_index_by_key =
        HashMap::<(usize, MetadataDocIndex, &[String]), usize>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, record) in records.iter().enumerate() {
        let contract_index =
            metadata_contract_index_to_usize(record.contract_index);
        let contract = &data.contracts[contract_index];
        let key = (
            contract.chain_index,
            contract.template_doc_index,
            record.doc.tokens.as_slice(),
        );
        if let Some(&atom_index) = atom_index_by_key.get(&key) {
            atoms[atom_index].members.push(record.contract_index);
            continue;
        }
        let atom_index = atoms.len();
        atom_index_by_key.insert(key, atom_index);
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(
                record_index,
            ),
            members: vec![record.contract_index],
            fallback_token_groups: Vec::new(),
        });
    }
    atoms
}

fn build_metadata_fallback_atoms(
    records: &[MetadataContentRecord],
    data: &MetadataData,
    contract_tokens: &[Vec<u32>],
) -> Vec<MetadataContentAtom> {
    let mut atom_index_by_key =
        HashMap::<(usize, MetadataDocIndex, &[String]), usize>::new();
    let mut token_group_index_by_atom = Vec::<HashMap<&[u32], usize>>::new();
    let mut atoms = Vec::<MetadataContentAtom>::new();
    for (record_index, record) in records.iter().enumerate() {
        let contract_index =
            metadata_contract_index_to_usize(record.contract_index);
        let contract = &data.contracts[contract_index];
        let key = (
            contract.chain_index,
            contract.template_doc_index,
            record.doc.tokens.as_slice(),
        );
        if let Some(&atom_index) = atom_index_by_key.get(&key) {
            let atom = &mut atoms[atom_index];
            atom.members.push(record.contract_index);
            let token_group_indexes = &mut token_group_index_by_atom[atom_index];
            let tokens = contract_tokens[contract_index].as_slice();
            if let Some(&token_group_index) = token_group_indexes.get(tokens) {
                atom.fallback_token_groups[token_group_index]
                    .members
                    .push(record.contract_index);
            } else {
                let token_group_index = atom.fallback_token_groups.len();
                token_group_indexes.insert(tokens, token_group_index);
                atom.fallback_token_groups.push(MetadataFallbackTokenGroup {
                    members: vec![record.contract_index],
                });
            }
            continue;
        }
        let atom_index = atoms.len();
        atom_index_by_key.insert(key, atom_index);
        token_group_index_by_atom.push(HashMap::from([(
            contract_tokens[contract_index].as_slice(),
            0,
        )]));
        atoms.push(MetadataContentAtom {
            chain_index: contract.chain_index,
            template_doc_index: contract.template_doc_index,
            representative_record_index: metadata_doc_index_from_usize(
                record_index,
            ),
            members: vec![record.contract_index],
            fallback_token_groups: vec![MetadataFallbackTokenGroup {
                members: vec![record.contract_index],
            }],
        });
    }
    atoms
}

fn metadata_content_atom_pair_matches(
    pair: (usize, MetadataDocIndex),
    atoms: &[MetadataContentAtom],
    records: &[MetadataContentRecord],
) -> bool {
    let (left, right) = pair;
    let left_record = metadata_doc_index_to_usize(
        atoms[left].representative_record_index,
    );
    let right_record = metadata_doc_index_to_usize(
        atoms[metadata_doc_index_to_usize(right)].representative_record_index,
    );
    metadata_content_pair_matches(
        &records[left_record].doc,
        &records[right_record].doc,
        METADATA_THRESHOLD,
    )
}

fn collect_metadata_content_atom_pair_hits(
    candidate_pairs: &[(usize, MetadataDocIndex)],
    atoms: &[MetadataContentAtom],
    records: &[MetadataContentRecord],
    pool: &rayon::ThreadPool,
) -> Vec<(usize, MetadataDocIndex)> {
    if candidate_pairs.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        pool.install(|| {
            candidate_pairs
                .par_iter()
                .copied()
                .filter(|&pair| {
                    metadata_content_atom_pair_matches(pair, atoms, records)
                })
                .collect()
        })
    } else {
        candidate_pairs
            .iter()
            .copied()
            .filter(|&pair| {
                metadata_content_atom_pair_matches(pair, atoms, records)
            })
            .collect()
    }
}

fn score_and_apply_metadata_atom_pair_batch(
    candidate_pairs: &mut Vec<(usize, MetadataDocIndex)>,
    atoms: &[MetadataContentAtom],
    records: &[MetadataContentRecord],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> u64 {
    if candidate_pairs.is_empty() {
        return 0;
    }
    let scored_pairs = candidate_pairs.len() as u64;
    let hits = collect_metadata_content_atom_pair_hits(
        candidate_pairs,
        atoms,
        records,
        context.pool,
    );
    candidate_pairs.clear();
    for (left, right) in hits {
        let left_atom = &atoms[left];
        let right_atom = &atoms[metadata_doc_index_to_usize(right)];
        let mut members =
            Vec::with_capacity(left_atom.members.len() + right_atom.members.len());
        members.extend_from_slice(&left_atom.members);
        members.extend_from_slice(&right_atom.members);
        apply_metadata_complete_match_group_union(
            context.data,
            context.chain_count,
            state,
            &members,
        );
    }
    scored_pairs
}

fn score_and_apply_metadata_fallback_atom_pair_batch(
    candidate_pairs: &mut Vec<(usize, MetadataDocIndex)>,
    atoms: &[MetadataContentAtom],
    records: &[MetadataContentRecord],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> u64 {
    if candidate_pairs.is_empty() {
        return 0;
    }
    let scored_pairs = candidate_pairs.len() as u64;
    let hits = collect_metadata_content_atom_pair_hits(
        candidate_pairs,
        atoms,
        records,
        context.pool,
    );
    candidate_pairs.clear();
    for (left, right) in hits {
        apply_metadata_fallback_atom_pair_union(
            &atoms[left],
            &atoms[metadata_doc_index_to_usize(right)],
            context,
            state,
        );
    }
    scored_pairs
}

#[cfg(test)]
fn collect_metadata_content_candidate_pairs(
    records: &[MetadataContentRecord],
    template_docs: &[MetadataDocIndex],
    template_matches: &MetadataTemplateMatches,
) -> Vec<(MetadataContractIndex, MetadataContractIndex)> {
    let index = MetadataContentCandidateIndex::new(records, template_docs);
    let mut scratch = MetadataCandidateScratch::new(records.len());
    let mut pairs = Vec::new();
    for left in 0..records.len().saturating_sub(1) {
        scratch.clear_for_next_left();
        index.append_candidates_after(
            left,
            &records[left],
            template_docs[left],
            template_matches,
            &mut scratch,
        );
        for &right in &scratch.candidates {
            pairs.push((
                records[left].contract_index,
                records[metadata_doc_index_to_usize(right)].contract_index,
            ));
        }
    }
    pairs.sort_unstable();
    pairs
}

fn union_metadata_shared_token_atoms(
    records: &[MetadataContentRecord],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms = build_metadata_content_atoms(records, context.data);
    let mut stats = MetadataContentUnionStats {
        atom_count: atoms.len(),
        ..MetadataContentUnionStats::default()
    };
    for atom in &atoms {
        apply_metadata_complete_match_group_union(
            context.data,
            context.chain_count,
            state,
            &atom.members,
        );
    }
    if atoms.len() < 2 {
        return stats;
    }
    let candidate_index = if atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        context.pool.install(|| {
            MetadataContentCandidateIndex::from_atoms_parallel(records, &atoms)
        })
    } else {
        MetadataContentCandidateIndex::from_atoms(records, &atoms)
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs =
        Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(
            left_atom.representative_record_index,
        );
        let left_contract_index =
            metadata_contract_index_to_usize(left_atom.members[0]);
        debug_assert_eq!(
            context.data.contracts[left_contract_index].chain_index,
            left_atom.chain_index
        );
        scratch.clear_for_next_left();
        candidate_index.append_candidates_after(
            left,
            &records[left_record_index],
            left_atom.template_doc_index,
            context.template_matches,
            &mut scratch,
        );
        stats.candidate_pairs = stats
            .candidate_pairs
            .saturating_add(scratch.candidates.len() as u64);
        for &right in &scratch.candidates {
            let right_atom = &atoms[metadata_doc_index_to_usize(right)];
            let right_contract_index =
                metadata_contract_index_to_usize(right_atom.members[0]);
            debug_assert!(context.template_matches.matches(
                metadata_doc_index_to_usize(left_atom.template_doc_index),
                metadata_doc_index_to_usize(right_atom.template_doc_index),
            ));
            let singleton_pair =
                left_atom.members.len() == 1 && right_atom.members.len() == 1;
            if !singleton_pair
                || !metadata_pair_already_connected(
                    context.data,
                    context.chain_count,
                    state,
                    left_contract_index,
                    right_contract_index,
                )
            {
                candidate_pairs.push((left, right));
                if candidate_pairs.len() >= METADATA_CONTENT_SCORE_BATCH_PAIRS {
                    stats.scored_pairs = stats.scored_pairs.saturating_add(
                        score_and_apply_metadata_atom_pair_batch(
                            &mut candidate_pairs,
                            &atoms,
                            records,
                            context,
                            state,
                        ),
                    );
                }
            }
        }
    }
    stats.scored_pairs = stats.scored_pairs.saturating_add(
        score_and_apply_metadata_atom_pair_batch(
            &mut candidate_pairs,
            &atoms,
            records,
            context,
            state,
        ),
    );
    stats
}

fn union_metadata_no_common_content_candidates(
    records: &[MetadataContentRecord],
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    let atoms =
        build_metadata_fallback_atoms(records, context.data, context.contract_tokens);
    let mut stats = MetadataContentUnionStats {
        atom_count: atoms.len(),
        ..MetadataContentUnionStats::default()
    };
    for atom in &atoms {
        apply_metadata_fallback_atom_internal_unions(atom, context, state);
    }
    if atoms.len() < 2 {
        return stats;
    }
    let candidate_index = if atoms.len() >= METADATA_CONTENT_PARALLEL_MIN_RECORDS {
        context.pool.install(|| {
            MetadataContentCandidateIndex::from_atoms_parallel(records, &atoms)
        })
    } else {
        MetadataContentCandidateIndex::from_atoms(records, &atoms)
    };
    let mut scratch = MetadataCandidateScratch::new(atoms.len());
    let mut candidate_pairs =
        Vec::with_capacity(METADATA_CONTENT_SCORE_BATCH_PAIRS);
    for left in 0..atoms.len().saturating_sub(1) {
        let left_atom = &atoms[left];
        let left_record_index = metadata_doc_index_to_usize(
            left_atom.representative_record_index,
        );
        scratch.clear_for_next_left();
        candidate_index.append_candidates_after(
            left,
            &records[left_record_index],
            left_atom.template_doc_index,
            context.template_matches,
            &mut scratch,
        );
        stats.candidate_pairs = stats
            .candidate_pairs
            .saturating_add(scratch.candidates.len() as u64);
        let left_contract_index =
            metadata_contract_index_to_usize(left_atom.members[0]);
        for &right in &scratch.candidates {
            let right_atom = &atoms[metadata_doc_index_to_usize(right)];
            let right_index =
                metadata_contract_index_to_usize(right_atom.members[0]);
            debug_assert!(context.template_matches.matches(
                metadata_doc_index_to_usize(left_atom.template_doc_index),
                metadata_doc_index_to_usize(right_atom.template_doc_index),
            ));
            let singleton_pair =
                left_atom.members.len() == 1 && right_atom.members.len() == 1;
            if singleton_pair
                && metadata_pair_already_connected(
                    context.data,
                    context.chain_count,
                    state,
                    left_contract_index,
                    right_index,
                )
            {
                continue;
            }
            if metadata_fallback_atoms_have_disjoint_token_groups(
                left_atom,
                right_atom,
                context.contract_tokens,
            ) {
                candidate_pairs.push((left, right));
                if candidate_pairs.len() >= METADATA_CONTENT_SCORE_BATCH_PAIRS {
                    stats.scored_pairs = stats.scored_pairs.saturating_add(
                        score_and_apply_metadata_fallback_atom_pair_batch(
                            &mut candidate_pairs,
                            &atoms,
                            records,
                            context,
                            state,
                        ),
                    );
                }
            }
        }
    }
    stats.scored_pairs = stats.scored_pairs.saturating_add(
        score_and_apply_metadata_fallback_atom_pair_batch(
            &mut candidate_pairs,
            &atoms,
            records,
            context,
            state,
        ),
    );
    stats
}

fn union_metadata_content_candidates(
    records: &[MetadataContentRecord],
    scope: MetadataContentScope,
    context: &MetadataContentUnionContext<'_>,
    state: &mut MetadataUnionState,
) -> MetadataContentUnionStats {
    match scope {
        MetadataContentScope::SharedToken => {
            union_metadata_shared_token_atoms(records, context, state)
        }
        MetadataContentScope::NoCommonToken => {
            union_metadata_no_common_content_candidates(records, context, state)
        }
    }
}

fn metadata_pair_already_connected(
    data: &MetadataData,
    chain_count: usize,
    state: &mut MetadataUnionState,
    left: usize,
    right: usize,
) -> bool {
    let left_chain = data.contracts[left].chain_index;
    let right_chain = data.contracts[right].chain_index;
    if left_chain == right_chain {
        return state.intra.find(left) == state.intra.find(right);
    }
    let cross_connected = state
        .cross
        .as_mut()
        .is_some_and(|cross| cross.connected(left, right));
    let (primary_chain, secondary_chain) = if left_chain < right_chain {
        (left_chain, right_chain)
    } else {
        (right_chain, left_chain)
    };
    let matrix_connected = state.chain_matrix.as_mut().is_some_and(|matrix| {
        matrix[chain_pair_index(primary_chain, secondary_chain, chain_count)]
            .connected(left, right)
    });
    cross_connected && matrix_connected
}

fn metadata_content_pair_score(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> f64 {
    metadata_single_document_score(left, right)
        .max(metadata_single_document_score(right, left))
}

fn metadata_single_document_score(
    query: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> f64 {
    if !metadata_string_docs_share_token(query, right) {
        return 0.0;
    }
    let numerator = metadata_single_corpus_bm25_score(query, right, right);
    let denominator = metadata_single_corpus_bm25_score(query, query, right);
    if denominator <= 0.0 {
        0.0
    } else {
        (numerator / denominator).clamp(0.0, 1.0)
    }
}

fn metadata_single_corpus_bm25_score(
    query: &MetadataBm25Document,
    doc: &MetadataBm25Document,
    corpus_doc: &MetadataBm25Document,
) -> f64 {
    if query.tokens.is_empty() || doc.tokens.is_empty() || corpus_doc.tokens.is_empty() {
        return 0.0;
    }
    let doc_len = doc.tokens.len() as f64;
    let avg_doc_len = corpus_doc.tokens.len() as f64;
    let norm =
        METADATA_BM25_K1 * (1.0 - METADATA_BM25_B + METADATA_BM25_B * doc_len / avg_doc_len);
    query
        .term_freqs
        .iter()
        .map(|(token, query_tf)| {
            let tf = doc.term_freqs.get(token).copied().unwrap_or(0) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let doc_freq = f64::from(corpus_doc.term_freqs.contains_key(token));
            let idf = ((1.0 - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
            *query_tf as f64 * idf * (tf * (METADATA_BM25_K1 + 1.0)) / (tf + norm)
        })
        .sum()
}

fn metadata_string_docs_share_token(
    left: &MetadataBm25Document,
    right: &MetadataBm25Document,
) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.unique_tokens.len() && right_index < right.unique_tokens.len() {
        match left.unique_tokens[left_index].cmp(&right.unique_tokens[right_index]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    false
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
        let mut tokens = metadata_bm25_tokens(document);
        if tokens.is_empty() {
            return None;
        }
        tokens.sort_unstable();
        let mut term_freqs = HashMap::new();
        for token in &tokens {
            *term_freqs.entry(token.clone()).or_insert(0usize) += 1;
        }
        let mut unique_tokens = tokens.clone();
        unique_tokens.sort_unstable();
        unique_tokens.dedup();
        Some(Self {
            tokens,
            unique_tokens,
            term_freqs,
        })
    }
}

impl InternedMetadataDoc {
    fn from_source_doc(doc: InternedMetadataSourceDoc) -> Self {
        Self {
            unique_tokens: doc.unique_tokens,
        }
    }

    #[cfg(test)]
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
    fn new(
        query: &InternedMetadataSourceDoc,
        corpus: &InternedMetadataCorpus,
        max_token_weights: &[f64],
        postings: &[Vec<MetadataDocIndex>],
    ) -> Self {
        let terms = query_terms_from_token_ids(&query.tokens);
        let self_score = bm25_score_terms(&terms, query, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        let candidate_tokens = metadata_bm25_candidate_prefix(
            &terms,
            denominator,
            max_token_weights,
            postings,
            METADATA_THRESHOLD,
        );
        Self {
            terms,
            denominator,
            candidate_tokens,
        }
    }
}

fn metadata_bm25_candidate_prefix(
    terms: &[(usize, usize)],
    denominator: f64,
    max_token_weights: &[f64],
    postings: &[Vec<MetadataDocIndex>],
    threshold: f64,
) -> Vec<usize> {
    let mut candidates = terms
        .iter()
        .filter_map(|&(token, query_tf)| {
            let max_weight = max_token_weights.get(token).copied().unwrap_or(0.0);
            let upper_bound = query_tf as f64 * max_weight;
            (upper_bound > 0.0).then_some((token, upper_bound, postings[token].len()))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Vec::new();
    }

    candidates.sort_unstable_by(|left, right| {
        let left_cost = left.2 as f64 / left.1;
        let right_cost = right.2 as f64 / right.1;
        left_cost
            .total_cmp(&right_cost)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut remaining_upper_bound = candidates
        .iter()
        .map(|(_, upper_bound, _)| upper_bound)
        .sum::<f64>();
    let required_score = threshold * denominator;
    let tolerance = f64::EPSILON
        * (remaining_upper_bound.abs() + required_score.abs() + 1.0)
        * candidates.len() as f64
        * 8.0;
    let mut prefix = Vec::new();
    for (token, upper_bound, _) in candidates {
        prefix.push(token);
        remaining_upper_bound = (remaining_upper_bound - upper_bound).max(0.0);
        if remaining_upper_bound + tolerance < required_score {
            break;
        }
    }
    prefix
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
    fn from_source_doc_entries(entries: Vec<SourceMetadataDocEntry>) -> Self {
        let token_ids = lexical_metadata_token_ids(&entries);
        let mut postings = vec![Vec::new(); token_ids.len()];
        let mut doc_weights = Vec::with_capacity(entries.len());
        let mut source_docs = Vec::with_capacity(entries.len());
        for (doc_index, entry) in entries.into_iter().enumerate() {
            doc_weights.push(entry.contracts.len());
            source_docs.push(InternedMetadataSourceDoc::from_metadata_doc(
                &entry.doc,
                &token_ids,
                &mut postings,
                doc_index,
            ));
        }
        for indices in &mut postings {
            indices.sort_unstable();
            indices.dedup();
        }
        let corpus =
            InternedMetadataCorpus::from_doc_weights(&doc_weights, &source_docs, token_ids.len());
        let mut token_hashes = vec![0u64; token_ids.len()];
        for (token, &token_id) in &token_ids {
            token_hashes[token_id] = stable_metadata_token_hash(token);
        }
        let sketches = source_docs
            .par_iter()
            .map(|doc| metadata_sketch_from_interned_document(doc, &corpus, &token_hashes))
            .collect::<Vec<_>>();
        let prepared_docs = source_docs
            .par_iter()
            .map(|doc| PreparedInternedMetadataDoc::new(doc, &corpus))
            .collect::<Vec<_>>();
        let mut max_token_weights = vec![0.0f64; token_ids.len()];
        for doc in &prepared_docs {
            for &(token, weight) in &doc.token_weights {
                max_token_weights[token] = max_token_weights[token].max(weight);
            }
        }
        let queries = source_docs
            .par_iter()
            .map(|doc| {
                PreparedInternedMetadataQuery::new(
                    doc,
                    &corpus,
                    &max_token_weights,
                    &postings,
                )
            })
            .collect::<Vec<_>>();
        let docs = source_docs
            .into_iter()
            .map(InternedMetadataDoc::from_source_doc)
            .collect();
        Self {
            docs,
            corpus,
            queries,
            prepared_docs,
            postings,
            sketches,
            #[cfg(test)]
            token_ids,
            #[cfg(test)]
            build_thread_count: rayon::current_num_threads(),
        }
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

fn metadata_prefilter_document_from_json(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }

    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut parts = BTreeSet::new();
            collect_metadata_prefilter_parts(&value, &mut parts);
            parts.into_iter().collect::<Vec<_>>().join(" ")
        }
        Err(_) => normalize_metadata_text(raw),
    }
}

fn collect_metadata_prefilter_parts(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key_norm = normalize_metadata_text(key);
                if key_norm.is_empty() {
                    continue;
                }
                if matches!(key_norm.as_str(), "metadata" | "rawmetadata" | "raw") {
                    collect_metadata_prefilter_parts(item, parts);
                } else if key_norm == "trait_type" {
                    push_metadata_prefilter_part(parts, &key_norm);
                    if let Some(text) = item.as_str() {
                        push_metadata_prefilter_part(parts, text);
                    }
                } else if metadata_prefilter_includes_value(&key_norm) {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_values(item, parts);
                } else {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_parts(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_parts(item, parts);
            }
        }
        _ => {}
    }
}

fn collect_metadata_prefilter_values(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => push_metadata_prefilter_part(parts, text),
        Value::Number(number) => push_metadata_prefilter_part(parts, &number.to_string()),
        Value::Bool(value) => push_metadata_prefilter_part(parts, &value.to_string()),
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                push_metadata_prefilter_part(parts, key);
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Null => {}
    }
}

fn metadata_prefilter_includes_value(key: &str) -> bool {
    matches!(
        key,
        "description"
            | "bio"
            | "story"
            | "lore"
            | "summary"
            | "about"
            | "seller_fee_basis_points"
            | "fee_recipient"
            | "royalty"
            | "royalties"
            | "creator"
            | "creators"
            | "compiler"
            | "license"
            | "collection"
            | "marketplace"
            | "contract"
            | "chain"
    )
}

fn push_metadata_prefilter_part(parts: &mut BTreeSet<String>, raw: &str) {
    let text = normalize_metadata_text(raw);
    if !text.is_empty() {
        parts.insert(text);
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

    #[test]
    fn metadata_prefilter_document_matches_top_contract_template_semantics() {
        let document = metadata_prefilter_document_from_json(
            r#"{
                "name": "Seed #1",
                "description": "Shared Story",
                "attributes": [
                    {"trait_type": "Background", "value": "Red"}
                ],
                "image": "ipfs://seed/1.png"
            }"#,
        );

        assert_eq!(
            document,
            "attributes background description image name shared story trait_type value"
        );
    }

    #[test]
    fn metadata_sketch_source_match_uses_anchor_or_hamming_distance() {
        let anchored_left = MetadataSketch {
            simhash: 0,
            anchors: vec![1, 3],
        };
        let anchored_right = MetadataSketch {
            simhash: u64::MAX,
            anchors: vec![3, 5],
        };
        let near_left = MetadataSketch {
            simhash: 1_u64 << 63,
            anchors: Vec::new(),
        };
        let near_right = MetadataSketch {
            simhash: (1_u64 << 63) | ((1_u64 << 31) - 1),
            anchors: Vec::new(),
        };
        let far_right = MetadataSketch {
            simhash: (1_u64 << 63) | ((1_u64 << 33) - 1),
            anchors: Vec::new(),
        };

        assert!(metadata_sketch_source_match(
            &anchored_left,
            &anchored_right,
            32
        ));
        assert!(metadata_sketch_source_match(&near_left, &near_right, 32));
        assert!(!metadata_sketch_source_match(&near_left, &far_right, 32));
    }

    #[test]
    fn metadata_content_pair_match_is_symmetric_and_thresholded() {
        let left = MetadataBm25Document::from_text("gold dragon rare").unwrap();
        let identical = MetadataBm25Document::from_text("gold dragon rare").unwrap();
        let unrelated = MetadataBm25Document::from_text("silver cat").unwrap();

        assert!(metadata_content_pair_matches(
            &left,
            &identical,
            METADATA_THRESHOLD
        ));
        assert!(!metadata_content_pair_matches(
            &left,
            &unrelated,
            METADATA_THRESHOLD
        ));
        assert_eq!(
            metadata_content_pair_score(&left, &identical),
            metadata_content_pair_score(&identical, &left)
        );
    }

    #[test]
    fn metadata_template_matches_accept_exact_or_scored_document_pairs() {
        let matches = MetadataTemplateMatches::from_pairs([(2usize, 5usize), (1, 4)]);

        assert!(matches.matches(3, 3));
        assert!(matches.matches(2, 5));
        assert!(matches.matches(5, 2));
        assert!(!matches.matches(2, 4));
    }

    #[test]
    fn lowest_common_metadata_token_uses_sorted_compact_ids() {
        assert_eq!(
            lowest_common_metadata_token(&[1, 4, 8, 13], &[2, 4, 7, 13]),
            Some(4)
        );
        assert_eq!(lowest_common_metadata_token(&[1, 3], &[2, 4]), None);
        assert_eq!(lowest_common_metadata_token(&[], &[1]), None);
    }

    #[test]
    fn compact_token_index_preserves_lexical_token_order() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TEMP TABLE analysis_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '10', '{"description":"a ten"}'),
                ('ethereum', '0xaaa', '2',  '{"description":"shared token"}'),
                ('ethereum', '0xbbb', '2',  '{"description":"shared token"}'),
                ('ethereum', '0xbbb', '3',  '{"description":"b three"}')
            ) AS t(chain, contract_address, token_id, metadata_json);
            "#,
        )
        .unwrap();
        let mut builder = MetadataDataBuilder::new(1);
        for contract_address in ["0xaaa", "0xbbb"] {
            let prefilter = "description".to_string();
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 2,
                content_document: "shared token".to_string(),
                doc: MetadataBm25Document::from_text(&prefilter).unwrap(),
                doc_key: prefilter,
            });
        }
        let data = builder.finish();
        let chains = vec!["ethereum".to_string()];

        prepare_metadata_contract_token_rows(&conn, &data, &chains).unwrap();
        let contract_tokens =
            load_metadata_contract_tokens(&conn, data.contracts.len()).unwrap();
        assert_eq!(contract_tokens, vec![vec![0, 1], vec![1, 2]]);
    }

    #[test]
    fn token_content_groups_union_matches_without_contract_pair_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TEMP TABLE analysis_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '1', '{"description":"different lower"}'),
                ('ethereum', '0xaaa', '2', '{"description":"gold dragon"}'),
                ('ethereum', '0xbbb', '2', '{"description":"gold dragon"}')
            ) AS t(chain, contract_address, token_id, metadata_json);
            "#,
        )
        .unwrap();
        let mut builder = MetadataDataBuilder::new(1);
        for contract_address in ["0xaaa", "0xbbb"] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 2,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let chains = vec!["ethereum".to_string()];
        prepare_metadata_contract_token_rows(&conn, &data, &chains).unwrap();
        let contract_tokens =
            load_metadata_contract_tokens(&conn, data.contracts.len()).unwrap();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let mut state = MetadataUnionState {
            intra: UnionFind::new(2),
            cross: None,
            chain_matrix: None,
        };
        let template_matches = MetadataTemplateMatches::default();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        union_metadata_token_content_matches(&conn, &context, &mut state)
            .unwrap();

        assert_eq!(state.intra.find(0), state.intra.find(1));
    }

    fn metadata_doc_entry(text: &str) -> SourceMetadataDocEntry {
        SourceMetadataDocEntry {
            doc: MetadataBm25Document::from_text(text).unwrap(),
            contracts: vec![0],
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
    fn metadata_doc_pair_hits_are_collected_for_left_range() {
        let docs = vec![
            metadata_doc_entry("gold dragon alpha"),
            metadata_doc_entry("dragon gold beta"),
            metadata_doc_entry("silver cat"),
            metadata_doc_entry("gold dragon beta"),
        ];
        let index = InternedMetadataIndex::from_source_doc_entries(docs);
        let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());

        let batch = collect_metadata_doc_pair_hits_for_left_range(
            1..3,
            MetadataPairScoringContext {
                docs: &index.docs,
                sketches: &index.sketches,
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
    fn metadata_doc_pair_prefilter_uses_sketch_instead_of_rare_anchor_gate() {
        let shared =
            "attributes image name trait_type value description external_url animation_url \
             metadata raw collection creator royalty license marketplace contract chain story \
             lore summary";
        let docs = vec![
            metadata_doc_entry(&format!("{shared} alpha")),
            metadata_doc_entry(&format!("{shared} beta")),
            metadata_doc_entry(&format!("{shared} gamma")),
        ];
        let index = InternedMetadataIndex::from_source_doc_entries(docs);
        let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());

        let batch = collect_metadata_doc_pair_hits_for_left_range(
            0..1,
            MetadataPairScoringContext {
                docs: &index.docs,
                sketches: &index.sketches,
                postings: &index.postings,
                queries: &index.queries,
                prepared_docs: &index.prepared_docs,
            },
            &scratch_pool,
        );

        assert!(batch.hits.contains(&(0, 1)));
    }

    #[test]
    fn metadata_bm25_prefix_candidates_are_selective_and_complete() {
        let shared = "attributes image name trait_type value description";
        let mut docs = vec![
            metadata_doc_entry(&format!(
                "{shared} copied_collection shared_story golden_dragon rare_anchor left_variant"
            )),
            metadata_doc_entry(&format!(
                "{shared} copied_collection shared_story golden_dragon rare_anchor right_variant"
            )),
        ];
        docs.extend(
            (0..96).map(|index| metadata_doc_entry(&format!("{shared} unrelated_{index}"))),
        );
        let index = InternedMetadataIndex::from_source_doc_entries(docs);
        let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());
        let context = MetadataPairScoringContext {
            docs: &index.docs,
            sketches: &index.sketches,
            postings: &index.postings,
            queries: &index.queries,
            prepared_docs: &index.prepared_docs,
        };
        let mut scratch = scratch_pool.take();

        let candidates =
            metadata_candidate_indices_for_left_with_scratch(0, &context, &mut scratch).to_vec();
        let brute_force_matches = (1..index.docs.len())
            .filter(|&right| {
                metadata_sketch_source_match(
                    &index.sketches[0],
                    &index.sketches[right],
                    METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
                ) && score_metadata_with_prepared_doc(
                    &index.queries[0],
                    &index.prepared_docs[right],
                ) >= METADATA_THRESHOLD
            })
            .map(metadata_doc_index_from_usize)
            .collect::<Vec<_>>();

        assert!(index.queries[0].candidate_tokens.len() < index.queries[0].terms.len());
        assert!(candidates.len() < index.docs.len() / 4);
        assert!(brute_force_matches
            .iter()
            .all(|right| candidates.contains(right)));
        assert!(brute_force_matches.contains(&metadata_doc_index_from_usize(1)));
    }

    #[test]
    fn metadata_bm25_prefix_pair_hits_equal_brute_force_results() {
        let docs = vec![
            metadata_doc_entry("attributes description gold dragon rare"),
            metadata_doc_entry("attributes description gold dragon"),
            metadata_doc_entry("attributes description silver dragon"),
            metadata_doc_entry("attributes image blue cat"),
            metadata_doc_entry("description gold dragon rare edition"),
            metadata_doc_entry("collection creator unrelated item"),
        ];
        let index = InternedMetadataIndex::from_source_doc_entries(docs);
        let context = MetadataPairScoringContext {
            docs: &index.docs,
            sketches: &index.sketches,
            postings: &index.postings,
            queries: &index.queries,
            prepared_docs: &index.prepared_docs,
        };
        let scratch_pool = MetadataCandidateScratchPool::new(index.docs.len());
        let actual = collect_metadata_doc_pair_hits_for_left_range(
            0..index.docs.len(),
            context,
            &scratch_pool,
        )
        .hits;
        let mut expected = Vec::new();
        for left in 0..index.docs.len() {
            for right in left + 1..index.docs.len() {
                if metadata_sketch_source_match(
                    &index.sketches[left],
                    &index.sketches[right],
                    METADATA_SKETCH_SOURCE_HAMMING_THRESHOLD,
                ) && (score_metadata_with_prepared_doc(
                    &index.queries[left],
                    &index.prepared_docs[right],
                ) >= METADATA_THRESHOLD
                    || score_metadata_with_prepared_doc(
                        &index.queries[right],
                        &index.prepared_docs[left],
                    ) >= METADATA_THRESHOLD)
                {
                    expected.push((left, right));
                }
            }
        }

        assert_eq!(actual, expected);
    }

    #[test]
    fn metadata_content_inverted_index_partitions_shared_terms_by_compatible_template() {
        let records = vec![
            MetadataContentRecord {
                contract_index: 0,
                doc: MetadataBm25Document::from_text("ipfs gold dragon").unwrap(),
            },
            MetadataContentRecord {
                contract_index: 1,
                doc: MetadataBm25Document::from_text("ipfs gold cat").unwrap(),
            },
            MetadataContentRecord {
                contract_index: 2,
                doc: MetadataBm25Document::from_text("ipfs silver bird").unwrap(),
            },
            MetadataContentRecord {
                contract_index: 3,
                doc: MetadataBm25Document::from_text("ipfs silver fox").unwrap(),
            },
        ];
        let template_docs = vec![0, 0, 1, 1];
        let candidates = collect_metadata_content_candidate_pairs(
            &records,
            &template_docs,
            &MetadataTemplateMatches::default(),
        );

        assert_eq!(candidates, vec![(0, 1), (2, 3)]);
    }

    #[test]
    fn metadata_content_pair_batch_parallel_scoring_keeps_only_matching_candidates() {
        let mut records = vec![MetadataContentRecord {
            contract_index: 0,
            doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
        }];
        for contract_index in 1..=METADATA_CONTENT_PARALLEL_MIN_RECORDS {
            let content = if contract_index % 2 == 0 {
                "gold dragon"
            } else {
                "silver cat"
            };
            records.push(MetadataContentRecord {
                contract_index: metadata_contract_index_from_usize(
                    contract_index,
                ),
                doc: MetadataBm25Document::from_text(content).unwrap(),
            });
        }
        let atoms = (0..records.len())
            .map(|index| MetadataContentAtom {
                chain_index: 0,
                template_doc_index: 0,
                representative_record_index: metadata_doc_index_from_usize(index),
                members: vec![metadata_contract_index_from_usize(index)],
                fallback_token_groups: Vec::new(),
            })
            .collect::<Vec<_>>();
        let candidates = (1..records.len())
            .map(|right| (0, metadata_doc_index_from_usize(right)))
            .collect::<Vec<_>>();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let hits = collect_metadata_content_atom_pair_hits(
            &candidates,
            &atoms,
            &records,
            &pool,
        );
        let expected = (2..=METADATA_CONTENT_PARALLEL_MIN_RECORDS)
            .step_by(2)
            .map(|right| (0, metadata_doc_index_from_usize(right)))
            .collect::<Vec<_>>();

        assert_eq!(hits, expected);
    }

    #[test]
    fn metadata_content_candidates_accept_matching_later_common_token() {
        let mut builder = MetadataDataBuilder::new(1);
        for contract_address in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = vec![vec![1, 4], vec![1, 4], vec![4], vec![4]];
        let records = vec![
            MetadataContentRecord {
                contract_index: 0,
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            },
            MetadataContentRecord {
                contract_index: 1,
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            },
            MetadataContentRecord {
                contract_index: 2,
                doc: MetadataBm25Document::from_text("silver cat").unwrap(),
            },
            MetadataContentRecord {
                contract_index: 3,
                doc: MetadataBm25Document::from_text("silver cat").unwrap(),
            },
        ];
        let mut state = MetadataUnionState {
            intra: UnionFind::new(4),
            cross: None,
            chain_matrix: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        union_metadata_content_candidates(
            &records,
            MetadataContentScope::SharedToken,
            &context,
            &mut state,
        );

        assert_eq!(state.intra.find(0), state.intra.find(1));
        assert_eq!(state.intra.find(2), state.intra.find(3));
    }

    #[test]
    fn metadata_content_union_collapses_identical_dense_component_to_one_atom() {
        let mut builder = MetadataDataBuilder::new(1);
        for contract_address in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = vec![vec![1], vec![1], vec![1], vec![1]];
        let records = (0..4)
            .map(|contract_index| MetadataContentRecord {
                contract_index,
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            })
            .collect::<Vec<_>>();
        let mut state = MetadataUnionState {
            intra: UnionFind::new(4),
            cross: None,
            chain_matrix: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::SharedToken,
            &context,
            &mut state,
        );

        assert_eq!(stats.atom_count, 1);
        assert_eq!(stats.candidate_pairs, 0);
        assert_eq!(stats.scored_pairs, 0);
        assert_eq!(state.intra.find(0), state.intra.find(3));
    }

    #[test]
    fn metadata_content_atoms_ignore_bm25_token_order() {
        let mut builder = MetadataDataBuilder::new(1);
        for contract_address in ["0xaaa", "0xbbb"] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: "gold dragon rare".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = vec![vec![1], vec![1]];
        let records = vec![
            MetadataContentRecord {
                contract_index: 0,
                doc: MetadataBm25Document::from_text("gold dragon rare").unwrap(),
            },
            MetadataContentRecord {
                contract_index: 1,
                doc: MetadataBm25Document::from_text("rare gold dragon").unwrap(),
            },
        ];
        let mut state = MetadataUnionState {
            intra: UnionFind::new(2),
            cross: None,
            chain_matrix: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::SharedToken,
            &context,
            &mut state,
        );

        assert_eq!(stats.atom_count, 1);
        assert_eq!(stats.scored_pairs, 0);
        assert_eq!(state.intra.find(0), state.intra.find(1));
    }

    #[test]
    fn metadata_content_atoms_preserve_cross_chain_matrix_membership() {
        let mut builder = MetadataDataBuilder::new(2);
        for (chain_index, contract_address) in [
            (0, "0xeth-a"),
            (0, "0xeth-b"),
            (1, "0xbase-a"),
            (1, "0xbase-b"),
        ] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = vec![vec![1], vec![1], vec![1], vec![1]];
        let records = (0..4)
            .map(|contract_index| MetadataContentRecord {
                contract_index,
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            })
            .collect::<Vec<_>>();
        let mut state = MetadataUnionState {
            intra: UnionFind::new(4),
            cross: Some(SparseUnionFind::default()),
            chain_matrix: Some(new_chain_matrix_reuse_states(1)),
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 2,
            pool: &pool,
        };

        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::SharedToken,
            &context,
            &mut state,
        );

        assert_eq!(stats.atom_count, 2);
        assert_eq!(stats.candidate_pairs, 1);
        assert_eq!(stats.scored_pairs, 1);
        assert_eq!(state.intra.find(0), state.intra.find(1));
        assert_eq!(state.intra.find(2), state.intra.find(3));
        let cross = state.cross.as_mut().unwrap();
        assert!(cross.connected(0, 3));
        assert!(cross.connected(1, 2));
        let matrix = state.chain_matrix.as_mut().unwrap();
        assert!(matrix[0].connected(0, 3));
        assert!(matrix[0].connected(1, 2));
    }

    #[test]
    fn metadata_content_atoms_expand_members_when_representatives_are_preconnected() {
        let mut builder = MetadataDataBuilder::new(2);
        for (chain_index, contract_address) in [
            (0, "0xeth-a"),
            (0, "0xeth-b"),
            (1, "0xbase-a"),
            (1, "0xbase-b"),
        ] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = vec![vec![1, 2], vec![2], vec![1, 2], vec![2]];
        let records = (0..4)
            .map(|contract_index| MetadataContentRecord {
                contract_index,
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            })
            .collect::<Vec<_>>();
        let mut state = MetadataUnionState {
            intra: UnionFind::new(4),
            cross: Some(SparseUnionFind::default()),
            chain_matrix: Some(new_chain_matrix_reuse_states(1)),
        };
        apply_metadata_contract_pair_union(&data, 2, &mut state, 0, 2);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 2,
            pool: &pool,
        };

        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::SharedToken,
            &context,
            &mut state,
        );

        assert_eq!(stats.atom_count, 2);
        assert_eq!(stats.candidate_pairs, 1);
        assert_eq!(stats.scored_pairs, 1);
        let cross = state.cross.as_mut().unwrap();
        assert!(cross.connected(1, 3));
        let matrix = state.chain_matrix.as_mut().unwrap();
        assert!(matrix[0].connected(1, 3));
    }

    #[test]
    fn metadata_representative_fallback_unions_only_without_common_token() {
        let mut builder = MetadataDataBuilder::new(1);
        for (contract_address, content) in [
            ("0xaaa", "gold dragon"),
            ("0xbbb", "gold dragon"),
            ("0xccc", "silver cat"),
            ("0xddd", "silver cat"),
        ] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: content.to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let contract_tokens = vec![vec![1], vec![2], vec![3], vec![3]];
        let mut state = MetadataUnionState {
            intra: UnionFind::new(4),
            cross: None,
            chain_matrix: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let template_matches = MetadataTemplateMatches::default();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        union_metadata_representative_content_fallback(&context, &mut state);

        assert_eq!(state.intra.find(0), state.intra.find(1));
        assert_ne!(state.intra.find(2), state.intra.find(3));
    }

    #[test]
    fn metadata_fallback_atoms_collapse_identical_nonempty_token_sets_without_unioning() {
        let mut builder = MetadataDataBuilder::new(1);
        for contract_address in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = vec![vec![1]; 4];
        let records = (0..4)
            .map(|contract_index| MetadataContentRecord {
                contract_index,
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            })
            .collect::<Vec<_>>();
        let mut state = MetadataUnionState {
            intra: UnionFind::new(4),
            cross: None,
            chain_matrix: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::NoCommonToken,
            &context,
            &mut state,
        );

        assert_eq!(stats.atom_count, 1);
        assert_eq!(stats.candidate_pairs, 0);
        assert_eq!(stats.scored_pairs, 0);
        assert_ne!(state.intra.find(0), state.intra.find(1));
    }

    #[test]
    fn metadata_fallback_atoms_union_identical_members_without_token_ids() {
        let mut builder = MetadataDataBuilder::new(1);
        for contract_address in ["0xaaa", "0xbbb", "0xccc", "0xddd"] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = vec![Vec::new(); 4];
        let records = (0..4)
            .map(|contract_index| MetadataContentRecord {
                contract_index,
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            })
            .collect::<Vec<_>>();
        let mut state = MetadataUnionState {
            intra: UnionFind::new(4),
            cross: None,
            chain_matrix: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::NoCommonToken,
            &context,
            &mut state,
        );

        assert_eq!(stats.atom_count, 1);
        assert_eq!(stats.candidate_pairs, 0);
        assert_eq!(stats.scored_pairs, 0);
        assert_eq!(state.intra.find(0), state.intra.find(3));
    }

    #[test]
    fn metadata_fallback_atoms_avoid_quadratic_pairs_for_disjoint_token_sets() {
        const CONTRACT_COUNT: usize = 128;
        let mut builder = MetadataDataBuilder::new(1);
        for index in 0..CONTRACT_COUNT {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: format!("0x{index:040x}"),
                nft_count: 1,
                content_document: "gold dragon".to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = (0..CONTRACT_COUNT)
            .map(|index| vec![u32::try_from(index).unwrap()])
            .collect::<Vec<_>>();
        let records = (0..CONTRACT_COUNT)
            .map(|contract_index| MetadataContentRecord {
                contract_index: metadata_contract_index_from_usize(contract_index),
                doc: MetadataBm25Document::from_text("gold dragon").unwrap(),
            })
            .collect::<Vec<_>>();
        let mut state = MetadataUnionState {
            intra: UnionFind::new(CONTRACT_COUNT),
            cross: None,
            chain_matrix: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 1,
            pool: &pool,
        };

        let stats = union_metadata_content_candidates(
            &records,
            MetadataContentScope::NoCommonToken,
            &context,
            &mut state,
        );

        assert_eq!(stats.atom_count, 1);
        assert_eq!(stats.candidate_pairs, 0);
        assert_eq!(stats.scored_pairs, 0);
        assert_eq!(state.intra.find(0), state.intra.find(CONTRACT_COUNT - 1));
    }

    #[test]
    fn metadata_fallback_atoms_match_brute_force_connectivity() {
        let fixtures = [
            (0, "0xeth-a", "gold dragon", vec![1]),
            (0, "0xeth-b", "gold dragon", vec![1, 2]),
            (0, "0xeth-c", "gold dragon", vec![2]),
            (0, "0xeth-d", "gold dragon rare", vec![3]),
            (1, "0xbase-a", "gold dragon", vec![1]),
            (1, "0xbase-b", "gold dragon", vec![4]),
            (1, "0xbase-c", "gold dragon rare", Vec::new()),
            (1, "0xbase-d", "silver cat", vec![5]),
        ];
        let mut builder = MetadataDataBuilder::new(2);
        for (chain_index, contract_address, content, _) in &fixtures {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: *chain_index,
                contract_address: (*contract_address).to_string(),
                nft_count: 1,
                content_document: (*content).to_string(),
                doc: MetadataBm25Document::from_text("shared template").unwrap(),
                doc_key: metadata_document_key("shared template"),
            });
        }
        let data = builder.finish();
        let template_matches = MetadataTemplateMatches::default();
        let contract_tokens = fixtures
            .iter()
            .map(|(_, _, _, tokens)| tokens.clone())
            .collect::<Vec<_>>();
        let records = fixtures
            .iter()
            .enumerate()
            .map(|(contract_index, (_, _, content, _))| MetadataContentRecord {
                contract_index: metadata_contract_index_from_usize(contract_index),
                doc: MetadataBm25Document::from_text(content).unwrap(),
            })
            .collect::<Vec<_>>();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let context = MetadataContentUnionContext {
            data: &data,
            template_matches: &template_matches,
            contract_tokens: &contract_tokens,
            chain_count: 2,
            pool: &pool,
        };
        let new_state = || MetadataUnionState {
            intra: UnionFind::new(fixtures.len()),
            cross: Some(SparseUnionFind::default()),
            chain_matrix: Some(new_chain_matrix_reuse_states(1)),
        };
        let mut optimized = new_state();
        union_metadata_content_candidates(
            &records,
            MetadataContentScope::NoCommonToken,
            &context,
            &mut optimized,
        );

        let mut reference = new_state();
        for left in 0..records.len() {
            for right in left + 1..records.len() {
                if lowest_common_metadata_token(
                    &contract_tokens[left],
                    &contract_tokens[right],
                )
                .is_none()
                    && metadata_content_pair_matches(
                        &records[left].doc,
                        &records[right].doc,
                        METADATA_THRESHOLD,
                    )
                {
                    apply_metadata_contract_pair_union(
                        &data,
                        2,
                        &mut reference,
                        left,
                        right,
                    );
                }
            }
        }

        for left in 0..records.len() {
            for right in left + 1..records.len() {
                assert_eq!(
                    optimized.intra.find(left) == optimized.intra.find(right),
                    reference.intra.find(left) == reference.intra.find(right),
                    "intra connectivity differs for {left}-{right}"
                );
                assert_eq!(
                    optimized.cross.as_mut().unwrap().connected(left, right),
                    reference.cross.as_mut().unwrap().connected(left, right),
                    "cross connectivity differs for {left}-{right}"
                );
                assert_eq!(
                    optimized.chain_matrix.as_mut().unwrap()[0]
                        .connected(left, right),
                    reference.chain_matrix.as_mut().unwrap()[0]
                        .connected(left, right),
                    "matrix connectivity differs for {left}-{right}"
                );
            }
        }
    }

    #[test]
    fn metadata_doc_pair_hits_score_one_left_with_reused_scratch() {
        let docs = vec![
            metadata_doc_entry("gold dragon alpha omega"),
            metadata_doc_entry("dragon gold alpha"),
            metadata_doc_entry("silver cat"),
            metadata_doc_entry("gold dragon omega"),
        ];
        let index = InternedMetadataIndex::from_source_doc_entries(docs);
        let mut scratch = MetadataCandidateScratch::new(index.docs.len());
        let mut hits = Vec::new();

        let candidate_pairs = collect_metadata_doc_pair_hits_for_left_with_scratch(
            0,
            &MetadataPairScoringContext {
                docs: &index.docs,
                sketches: &index.sketches,
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
    fn metadata_candidate_scratch_deduplicates_selected_prefix_postings() {
        let mut scratch = MetadataCandidateScratch::new(3);
        scratch.clear_for_next_left();
        append_metadata_posting_except(&[0, 1, 2], 0, &mut scratch);
        append_metadata_posting_except(&[0, 1], 0, &mut scratch);

        assert_eq!(scratch.candidates, vec![1, 2]);
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
        let index = InternedMetadataIndex::from_source_doc_entries(docs);
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
    fn metadata_bm25_index_builds_top_contract_sketches() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("dragon silver"),
            metadata_doc_entry("cat"),
        ];

        let index = InternedMetadataIndex::from_source_doc_entries(docs);

        assert_eq!(index.sketches.len(), 3);
        assert!(index.sketches.iter().all(|sketch| sketch.simhash != 0));
        assert!(index
            .sketches
            .iter()
            .all(|sketch| sketch.anchors.len() <= METADATA_SKETCH_ANCHOR_COUNT));
    }

    #[test]
    fn metadata_bm25_index_assigns_lexical_token_ids_for_stable_score_order() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("silver cat"),
        ];
        let index = InternedMetadataIndex::from_source_doc_entries(docs);

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

        let index = InternedMetadataIndex::from_source_doc_entries(docs);

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
            content_document: "gold dragon".to_string(),
            doc,
            doc_key,
        });

        let data = builder.finish();

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
            content_document: "gold dragon".to_string(),
            doc,
            doc_key,
        });

        let data = builder.finish();

        let _: &[MetadataContractIndex] = data.contracts_by_chain[0].as_slice();
    }

    #[test]
    fn metadata_contracts_keep_their_template_document_index() {
        let mut builder = MetadataDataBuilder::new(1);
        for (contract_address, document) in [
            ("0xaaa", "gold dragon"),
            ("0xbbb", "gold dragon"),
            ("0xccc", "silver cat"),
        ] {
            builder.merge_indexed_row(IndexedMetadataRow {
                chain_index: 0,
                contract_address: contract_address.to_string(),
                nft_count: 1,
                content_document: document.to_string(),
                doc: MetadataBm25Document::from_text(document).unwrap(),
                doc_key: metadata_document_key(document),
            });
        }

        let data = builder.finish();

        assert_eq!(
            data.contracts
                .iter()
                .map(|contract| contract.template_doc_index)
                .collect::<Vec<_>>(),
            vec![0, 0, 1]
        );
    }

    #[test]
    fn metadata_index_consumes_source_docs_without_retaining_contract_memberships() {
        let docs = vec![metadata_doc_entry("gold dragon")];

        let index = InternedMetadataIndex::from_source_doc_entries(docs);

        assert_eq!(index.docs.len(), 1);
        assert!(index.token_id("gold").is_some());
    }

    #[test]
    fn interned_metadata_index_keeps_only_compact_candidate_docs_after_preparation() {
        let docs = vec![metadata_doc_entry("gold dragon gold")];

        let index = InternedMetadataIndex::from_source_doc_entries(docs);

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

    #[test]
    fn metadata_index_build_uses_configured_rayon_pool() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TEMP TABLE analysis_rows AS
            SELECT * FROM (
                VALUES
                ('ethereum', '0xaaa', '1', '{"description":"gold dragon"}'),
                ('ethereum', '0xbbb', '1', '{"description":"silver cat"}')
            ) AS t(chain, contract_address, token_id, metadata_json);
            "#,
        )
        .unwrap();
        let global_threads = rayon::current_num_threads();
        let configured_threads = if global_threads == 1 { 2 } else { 1 };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(configured_threads)
            .build()
            .unwrap();

        let data = load_metadata_data(
            &conn,
            &["ethereum".to_string()],
            &pool,
        )
        .unwrap();

        assert_eq!(
            data.metadata_index.build_thread_count,
            configured_threads
        );
    }

    #[test]
    fn metadata_raw_row_builds_distinct_prefilter_and_content_documents() {
        let chains = ["ethereum".to_string()];
        let chain_indexes = chains
            .iter()
            .enumerate()
            .map(|(index, chain)| (chain.as_str(), index))
            .collect::<HashMap<_, _>>();
        let rows = vec![RawMetadataRow {
            chain: "ethereum".into(),
            contract_address: "0xaaa".into(),
            metadata_json: r#"{
                "name":"Alpha #1",
                "image":"ipfs://alpha/1.png",
                "attributes":[{"trait_type":"Background","value":"Blue"}]
            }"#
            .into(),
            nft_count: 2,
        }];

        let indexed = index_metadata_raw_row_chunk(rows, &chain_indexes);

        assert_eq!(
            indexed[0].doc.tokens.join(" "),
            "attributes background image name trait_type value"
        );
        assert!(indexed[0].content_document.contains("ipfs://alpha/1.png"));
        assert!(indexed[0].content_document.contains("blue"));
        assert!(!indexed[0].content_document.contains("alpha #1"));
    }

}
