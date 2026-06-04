use std::collections::{BTreeSet, HashSet};

use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

const METADATA_THRESHOLD: f64 = 0.6;
const METADATA_MATCH_MODE: &str = "bm25_representative";
const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;
const METADATA_BM25_K1: f64 = 1.2;
const METADATA_BM25_B: f64 = 0.75;
const METADATA_LOAD_CHUNK_ROWS: usize = 1024;
const METADATA_PAIR_LEFT_CHUNK_SIZE: usize = 64;
type MetadataDocKey = Vec<(String, usize)>;

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
    contracts: Vec<usize>,
    contracts_by_chain: Vec<Vec<usize>>,
}

#[derive(Debug)]
struct MetadataDocEntry {
    contracts: Vec<usize>,
    contracts_by_chain: Vec<Vec<usize>>,
}

#[derive(Debug)]
struct MetadataData {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<usize>>,
    docs: Vec<MetadataDocEntry>,
    metadata_index: InternedMetadataIndex,
}

struct MetadataDataBuilder {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<usize>>,
    contract_index_by_key: HashMap<(usize, String), usize>,
    docs: Vec<SourceMetadataDocEntry>,
    doc_index_by_key: HashMap<MetadataDocKey, usize>,
    doc_contract_memberships: HashSet<(usize, usize)>,
    chain_count: usize,
}

#[derive(Debug, Clone)]
struct MetadataBm25Document {
    tokens: Vec<String>,
    term_freqs: HashMap<String, usize>,
    unique_tokens: Vec<String>,
}

#[derive(Debug)]
struct InternedMetadataDoc {
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
struct InternedMetadataIndex {
    docs: Vec<InternedMetadataDoc>,
    corpus: InternedMetadataCorpus,
    queries: Vec<PreparedInternedMetadataQuery>,
    postings: Vec<Vec<usize>>,
    #[cfg(test)]
    token_ids: HashMap<String, usize>,
}

struct MetadataCandidateScratch {
    seen_generation: Vec<u32>,
    generation: u32,
    candidates: Vec<usize>,
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
            doc_contract_memberships: HashSet::new(),
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
                self.contracts_by_chain[row.chain_index].push(index);
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

        if self
            .doc_contract_memberships
            .insert((doc_index, contract_index))
        {
            self.docs[doc_index].contracts.push(contract_index);
            self.docs[doc_index].contracts_by_chain[row.chain_index].push(contract_index);
        }
    }

    fn finish(self) -> MetadataData {
        let metadata_index = InternedMetadataIndex::from_doc_entries(&self.docs);
        let docs = self
            .docs
            .into_iter()
            .map(|entry| MetadataDocEntry {
                contracts: entry.contracts,
                contracts_by_chain: entry.contracts_by_chain,
            })
            .collect();
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

    fn push_once(&mut self, index: usize) {
        if self.seen_generation[index] == self.generation {
            return;
        }
        self.seen_generation[index] = self.generation;
        self.candidates.push(index);
    }
}

fn run_metadata_analysis(
    conn: &Connection,
    chains: &[String],
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
    let totals = metadata_totals(&data, chains);
    let mut rows = Vec::new();
    if data.contracts.len() < 2 || data.docs.is_empty() {
        push_empty_metadata_rows(&mut rows, chains, &totals);
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
    pool.install(|| union_metadata_pairs(&data, chains.len(), &mut state));
    progress.step("metadata documents scored");
    push_metadata_summary_rows(&mut rows, &data, chains, &totals, &mut state);
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
            let indexed_rows = pool.install(|| index_metadata_raw_row_chunk(chunk, &chain_indexes));
            builder.merge_indexed_rows(indexed_rows);
        }
    }

    if !raw_rows.is_empty() {
        let indexed_rows = pool.install(|| index_metadata_raw_row_chunk(raw_rows, &chain_indexes));
        builder.merge_indexed_rows(indexed_rows);
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
                SELECT chain,
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
                GROUP BY chain, contract_address, metadata_json
            )
            SELECT m.chain,
                   m.contract_address,
                   m.metadata_json,
                   t.nft_count
            FROM eligible_metadata m
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
            let doc_key = metadata_document_key(&doc);
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

fn metadata_document_key(doc: &MetadataBm25Document) -> MetadataDocKey {
    doc.unique_tokens
        .iter()
        .map(|token| (token.clone(), doc.term_frequency(token)))
        .collect()
}

fn metadata_document_has_informative_token(doc: &MetadataBm25Document) -> bool {
    doc.unique_tokens
        .iter()
        .any(|token| !is_metadata_prefilter_key_token(token))
}

fn is_metadata_prefilter_key_token(token: &str) -> bool {
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
            | "royalties"
            | "royalty"
            | "seller_fee_basis_points"
            | "story"
            | "summary"
            | "trait_type"
            | "value"
    )
}

fn metadata_totals(data: &MetadataData, chains: &[String]) -> HashMap<String, NameTotals> {
    let mut totals = chains
        .iter()
        .map(|chain| {
            (
                chain.clone(),
                NameTotals {
                    contracts: 0,
                    nfts: 0,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    for contract in &data.contracts {
        let Some(total) = totals.get_mut(&chains[contract.chain_index]) else {
            continue;
        };
        total.contracts += 1;
        total.nfts += contract.nft_count;
    }
    totals
}

fn union_metadata_pairs(data: &MetadataData, chain_count: usize, state: &mut MetadataUnionState) {
    let index = &data.metadata_index;
    if index.corpus.total_docs == 0 {
        return;
    }
    for doc_index in 0..data.docs.len() {
        apply_metadata_exact_doc_unions(data, chain_count, state, doc_index);
    }

    let scoring_left_count = data.docs.len().saturating_sub(1);
    for left_start in (0..scoring_left_count).step_by(METADATA_PAIR_LEFT_CHUNK_SIZE) {
        let left_end = (left_start + METADATA_PAIR_LEFT_CHUNK_SIZE).min(scoring_left_count);
        let hits = collect_metadata_doc_pair_hits_for_left_range(
            left_start..left_end,
            &index.docs,
            &index.queries,
            &index.corpus,
            &index.postings,
        );
        for (left, right) in hits {
            apply_metadata_doc_pair_union(data, chain_count, state, left, right);
        }
    }
}

fn collect_metadata_doc_pair_hits_for_left_range(
    left_range: std::ops::Range<usize>,
    docs: &[InternedMetadataDoc],
    queries: &[PreparedInternedMetadataQuery],
    corpus: &InternedMetadataCorpus,
    postings: &[Vec<usize>],
) -> Vec<(usize, usize)> {
    let mut hits = left_range
        .into_par_iter()
        .fold(
            || (Vec::new(), MetadataCandidateScratch::new(docs.len())),
            |(mut local_hits, mut scratch), left| {
                let candidates = metadata_candidate_indices_for_left_with_scratch(
                    left,
                    &docs[left],
                    postings,
                    &mut scratch,
                );
                for &right in candidates {
                    if metadata_pair_score(left, right, docs, queries, corpus) >= METADATA_THRESHOLD
                    {
                        local_hits.push((left, right));
                    }
                }
                (local_hits, scratch)
            },
        )
        .map(|(hits, _scratch)| hits)
        .reduce(Vec::new, |mut left, mut right| {
            left.append(&mut right);
            left
        });
    hits.sort_unstable();
    hits.dedup();
    hits
}

fn metadata_candidate_indices_for_left_with_scratch<'a>(
    left: usize,
    doc: &InternedMetadataDoc,
    postings: &[Vec<usize>],
    scratch: &'a mut MetadataCandidateScratch,
) -> &'a [usize] {
    scratch.clear_for_next_left();
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
    docs: &[InternedMetadataDoc],
    queries: &[PreparedInternedMetadataQuery],
    corpus: &InternedMetadataCorpus,
) -> f64 {
    let left_to_right = score_metadata_with_query(&queries[left], &docs[right], corpus);
    if left_to_right >= METADATA_THRESHOLD {
        return left_to_right;
    }
    score_metadata_with_query(&queries[right], &docs[left], corpus)
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

fn union_dense_contract_group(union_find: &mut UnionFind, contracts: &[usize]) {
    let Some((&anchor, rest)) = contracts.split_first() else {
        return;
    };
    for &contract in rest {
        union_find.union(anchor, contract);
    }
}

fn union_dense_bipartite_contract_groups(
    union_find: &mut UnionFind,
    left_contracts: &[usize],
    right_contracts: &[usize],
) {
    let Some(&anchor) = left_contracts.first() else {
        return;
    };
    for &contract in &left_contracts[1..] {
        union_find.union(anchor, contract);
    }
    for &contract in right_contracts {
        union_find.union(anchor, contract);
    }
}

fn union_sparse_contract_group_when_multiple_chains(
    union_find: &mut SparseUnionFind,
    contracts: &[usize],
    contracts_by_chain: &[Vec<usize>],
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
    for &contract in rest {
        union_find.union(anchor, contract);
    }
}

fn union_sparse_bipartite_contract_groups(
    union_find: &mut SparseUnionFind,
    left_contracts: &[usize],
    right_contracts: &[usize],
) {
    let Some(&anchor) = left_contracts.first() else {
        return;
    };
    for &contract in &left_contracts[1..] {
        union_find.union(anchor, contract);
    }
    for &contract in right_contracts {
        union_find.union(anchor, contract);
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
    primary_contracts: &[usize],
    union_find: &mut UnionFind,
    scratch: &mut MetadataDenseComponentScratch,
) -> GroupSummary {
    for &index in primary_contracts {
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
        let mut term_freqs = HashMap::new();
        for token in &tokens {
            *term_freqs.entry(token.clone()).or_insert(0usize) += 1;
        }
        let mut unique_tokens = term_freqs.keys().cloned().collect::<Vec<_>>();
        unique_tokens.sort_unstable();
        Some(Self {
            tokens,
            term_freqs,
            unique_tokens,
        })
    }

    fn term_frequency(&self, token: &str) -> usize {
        *self.term_freqs.get(token).unwrap_or(&0)
    }
}

impl InternedMetadataDoc {
    fn from_metadata_doc(
        doc: &MetadataBm25Document,
        token_ids: &HashMap<String, usize>,
        postings: &mut [Vec<usize>],
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
        for &token_id in &unique_tokens {
            postings[token_id].push(doc_index);
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

impl InternedMetadataCorpus {
    fn from_doc_entries(
        entries: &[SourceMetadataDocEntry],
        docs: &[InternedMetadataDoc],
        token_count: usize,
    ) -> Self {
        let mut total_docs = 0usize;
        let mut total_terms = 0usize;
        let mut doc_freqs = vec![0; token_count];
        for (entry, doc) in entries.iter().zip(docs) {
            let weight = entry.contracts.len();
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
    fn new(query: &InternedMetadataDoc, corpus: &InternedMetadataCorpus) -> Self {
        let terms = query_terms_from_token_ids(&query.tokens);
        let self_score = bm25_score_terms(&terms, query, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        Self { terms, denominator }
    }

    fn has_term_overlap(&self, document: &InternedMetadataDoc) -> bool {
        self.terms
            .iter()
            .any(|(token, _)| document.term_frequency(*token) > 0)
    }
}

impl InternedMetadataIndex {
    fn from_doc_entries(entries: &[SourceMetadataDocEntry]) -> Self {
        let token_ids = lexical_metadata_token_ids(entries);
        let mut postings = vec![Vec::new(); token_ids.len()];
        let docs = entries
            .iter()
            .enumerate()
            .map(|(doc_index, entry)| {
                InternedMetadataDoc::from_metadata_doc(
                    &entry.doc,
                    &token_ids,
                    &mut postings,
                    doc_index,
                )
            })
            .collect::<Vec<_>>();
        for indices in &mut postings {
            indices.sort_unstable();
            indices.dedup();
        }
        let corpus = InternedMetadataCorpus::from_doc_entries(entries, &docs, token_ids.len());
        let queries = docs
            .par_iter()
            .map(|doc| PreparedInternedMetadataQuery::new(doc, &corpus))
            .collect::<Vec<_>>();
        Self {
            docs,
            corpus,
            queries,
            postings,
            #[cfg(test)]
            token_ids,
        }
    }

    #[cfg(test)]
    fn token_id(&self, token: &str) -> Option<usize> {
        self.token_ids.get(token).copied()
    }
}

fn score_metadata_with_query(
    query: &PreparedInternedMetadataQuery,
    right: &InternedMetadataDoc,
    corpus: &InternedMetadataCorpus,
) -> f64 {
    if !query.has_term_overlap(right) {
        return 0.0;
    }
    (bm25_score_terms(&query.terms, right, corpus) / query.denominator).clamp(0.0, 1.0)
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
    doc: &InternedMetadataDoc,
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
            let mut parts = BTreeSet::new();
            collect_metadata_prefilter_parts(&value, &mut parts);
            parts.into_iter().collect::<Vec<_>>().join(" ")
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

fn collect_metadata_prefilter_parts(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key_norm = normalize_metadata_text(key);
                if key_norm.is_empty() {
                    continue;
                }
                if is_structure_wrapper_key(&key_norm) {
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
    is_description_key(key) || is_platform_key(key)
}

fn push_metadata_prefilter_part(parts: &mut BTreeSet<String>, raw: &str) {
    let text = normalize_metadata_text(raw);
    if !text.is_empty() {
        parts.insert(text);
    }
}

fn is_structure_wrapper_key(key: &str) -> bool {
    matches!(key, "metadata" | "rawmetadata" | "raw")
}

fn is_description_key(key: &str) -> bool {
    matches!(
        key,
        "description" | "bio" | "story" | "lore" | "summary" | "about"
    )
}

fn is_platform_key(key: &str) -> bool {
    matches!(
        key,
        "seller_fee_basis_points"
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
    fn metadata_document_key_deduplicates_reordered_token_multisets() {
        let left = MetadataBm25Document::from_text("gold dragon gold").unwrap();
        let right = MetadataBm25Document::from_text("dragon gold gold").unwrap();

        assert_eq!(metadata_document_key(&left), metadata_document_key(&right));
    }

    #[test]
    fn metadata_document_uses_top_contract_prefilter_parts() {
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

        assert_eq!(
            document,
            "500 attributes background description eyes gold dragon irrelevant seller_fee_basis_points trait_type value"
        );
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
    fn metadata_document_normalizes_nfkc_like_top_contract_prefilter() {
        let document = metadata_document_from_json(
            r#"{"description":"\uFF27\uFF4F\uFF4C\uFF44\u3000Dragon"}"#,
        );

        assert_eq!(document, "description gold dragon");
    }

    #[test]
    fn metadata_document_requires_informative_prefilter_token_for_indexing() {
        let key_only = MetadataBm25Document::from_text("description").unwrap();
        let informative = MetadataBm25Document::from_text("description gold dragon").unwrap();

        assert!(!metadata_document_has_informative_token(&key_only));
        assert!(metadata_document_has_informative_token(&informative));
    }

    #[test]
    fn metadata_doc_pair_hits_are_collected_for_left_range() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("dragon gold"),
            metadata_doc_entry("silver cat"),
            metadata_doc_entry("gold dragon"),
        ];
        let index = InternedMetadataIndex::from_doc_entries(&docs);

        let hits = collect_metadata_doc_pair_hits_for_left_range(
            1..3,
            &index.docs,
            &index.queries,
            &index.corpus,
            &index.postings,
        );

        assert_eq!(hits, vec![(1, 3)]);
    }

    #[test]
    fn metadata_candidate_scratch_deduplicates_posting_hits() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("dragon gold"),
            metadata_doc_entry("gold cat"),
        ];
        let index = InternedMetadataIndex::from_doc_entries(&docs);
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
    fn metadata_bm25_index_interns_tokens_and_integer_postings() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("dragon gold"),
            metadata_doc_entry("silver cat"),
        ];
        let index = InternedMetadataIndex::from_doc_entries(&docs);
        let gold = index.token_id("gold").unwrap();
        let dragon = index.token_id("dragon").unwrap();

        assert_eq!(index.postings[gold], vec![0, 1]);
        assert_eq!(index.postings[dragon], vec![0, 1]);
        assert_eq!(index.docs[0].term_frequency(gold), 1);
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
        let index = InternedMetadataIndex::from_doc_entries(&docs);

        assert!(
            index.token_id("cat").unwrap()
                < index.token_id("dragon").unwrap()
                && index.token_id("dragon").unwrap() < index.token_id("gold").unwrap()
                && index.token_id("gold").unwrap() < index.token_id("silver").unwrap()
        );
    }

    #[test]
    fn metadata_data_builder_moves_bm25_documents_into_interned_index() {
        let mut builder = MetadataDataBuilder::new(1);
        let doc = MetadataBm25Document::from_text("gold dragon").unwrap();
        let doc_key = metadata_document_key(&doc);
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
