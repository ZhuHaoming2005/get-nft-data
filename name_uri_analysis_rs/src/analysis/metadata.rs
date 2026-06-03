use std::collections::HashSet;

use serde_json::Value;

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

#[derive(Clone, Debug)]
struct MetadataDocEntry {
    doc: MetadataBm25Document,
    contracts: Vec<usize>,
    contracts_by_chain: Vec<Vec<usize>>,
}

#[derive(Clone, Debug)]
struct MetadataData {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<usize>>,
    docs: Vec<MetadataDocEntry>,
}

struct MetadataDataBuilder {
    contracts: Vec<MetadataContract>,
    contracts_by_chain: Vec<Vec<usize>>,
    contract_index_by_key: HashMap<(usize, String), usize>,
    docs: Vec<MetadataDocEntry>,
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
struct MetadataBm25Corpus {
    total_docs: usize,
    avg_doc_len: f64,
    doc_freqs: HashMap<String, usize>,
}

#[derive(Debug)]
struct PreparedMetadataQuery {
    terms: Vec<(String, usize)>,
    denominator: f64,
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
                self.docs.push(MetadataDocEntry {
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
        MetadataData {
            contracts: self.contracts,
            contracts_by_chain: self.contracts_by_chain,
            docs: self.docs,
        }
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
    let mut stmt = conn.prepare(
        &format!(
            "
            WITH totals AS (
                SELECT chain, contract_address, count(*)::BIGINT AS nft_count
                FROM analysis_rows
                WHERE contract_address <> ''
                GROUP BY chain, contract_address
            )
            SELECT r.chain,
                   r.contract_address,
                   r.metadata_json,
                   t.nft_count
            FROM analysis_rows r
            JOIN totals t
              ON t.chain = r.chain
             AND t.contract_address = r.contract_address
            WHERE r.contract_address <> ''
              AND r.metadata_json <> ''
              AND length(r.metadata_json) <= {MAX_METADATA_BYTES_FOR_DEDUP}
              AND (
                  starts_with(r.metadata_json, '{{')
                  OR starts_with(r.metadata_json, '[')
              )
            ORDER BY r.chain, r.contract_address, r.rowid
            "
        ),
    )?;
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
    let corpus = MetadataBm25Corpus::from_doc_entries(&data.docs);
    if corpus.total_docs == 0 {
        return;
    }
    for doc_index in 0..data.docs.len() {
        apply_metadata_exact_doc_unions(data, chain_count, state, doc_index);
    }

    let queries = data
        .docs
        .par_iter()
        .map(|entry| PreparedMetadataQuery::new(&entry.doc, &corpus))
        .collect::<Vec<_>>();
    let postings = metadata_token_postings(&data.docs);
    let scoring_left_count = data.docs.len().saturating_sub(1);
    for left_start in (0..scoring_left_count).step_by(METADATA_PAIR_LEFT_CHUNK_SIZE) {
        let left_end = (left_start + METADATA_PAIR_LEFT_CHUNK_SIZE).min(scoring_left_count);
        let hits = collect_metadata_doc_pair_hits_for_left_range(
            left_start..left_end,
            &data.docs,
            &queries,
            &corpus,
            &postings,
        );
        for (left, right) in hits {
            apply_metadata_doc_pair_union(data, chain_count, state, left, right);
        }
    }
}

fn collect_metadata_doc_pair_hits_for_left_range(
    left_range: std::ops::Range<usize>,
    docs: &[MetadataDocEntry],
    queries: &[PreparedMetadataQuery],
    corpus: &MetadataBm25Corpus,
    postings: &HashMap<String, Vec<usize>>,
) -> Vec<(usize, usize)> {
    let mut hits = left_range
        .into_par_iter()
        .fold(Vec::new, |mut local_hits, left| {
            let candidates = metadata_candidate_indices_for_left(left, &docs[left].doc, postings);
            for right in candidates {
                if metadata_pair_score(left, right, docs, queries, corpus) >= METADATA_THRESHOLD {
                    local_hits.push((left, right));
                }
            }
            local_hits
        })
        .reduce(Vec::new, |mut left, mut right| {
            left.append(&mut right);
            left
        });
    hits.sort_unstable();
    hits.dedup();
    hits
}

fn metadata_pair_score(
    left: usize,
    right: usize,
    docs: &[MetadataDocEntry],
    queries: &[PreparedMetadataQuery],
    corpus: &MetadataBm25Corpus,
) -> f64 {
    let left_to_right = score_metadata_with_query(&queries[left], &docs[right].doc, corpus);
    if left_to_right >= METADATA_THRESHOLD {
        return left_to_right;
    }
    score_metadata_with_query(&queries[right], &docs[left].doc, corpus)
}

fn metadata_token_postings(docs: &[MetadataDocEntry]) -> HashMap<String, Vec<usize>> {
    let mut postings = docs
        .par_iter()
        .enumerate()
        .fold(HashMap::<String, Vec<usize>>::new, |mut local, (index, entry)| {
            for token in entry.doc.unique_tokens() {
                local.entry(token.clone()).or_default().push(index);
            }
            local
        })
        .reduce(HashMap::<String, Vec<usize>>::new, |mut left, right| {
            for (token, mut indices) in right {
                left.entry(token).or_default().append(&mut indices);
            }
            left
        });
    for indices in postings.values_mut() {
        indices.sort_unstable();
    }
    postings
}

fn metadata_candidate_indices_for_left(
    left: usize,
    doc: &MetadataBm25Document,
    postings: &HashMap<String, Vec<usize>>,
) -> Vec<usize> {
    let mut candidates = Vec::new();
    for token in doc.unique_tokens() {
        let Some(indices) = postings.get(token) else {
            continue;
        };
        for &right in indices {
            if right <= left {
                continue;
            }
            candidates.push(right);
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
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

    fn len(&self) -> usize {
        self.tokens.len()
    }

    fn term_frequency(&self, token: &str) -> usize {
        *self.term_freqs.get(token).unwrap_or(&0)
    }

    fn unique_tokens(&self) -> &[String] {
        &self.unique_tokens
    }
}

impl MetadataBm25Corpus {
    fn from_doc_entries(entries: &[MetadataDocEntry]) -> Self {
        let mut total_docs = 0usize;
        let mut total_terms = 0usize;
        let mut doc_freqs = HashMap::new();
        for entry in entries {
            let weight = entry.contracts.len();
            if weight == 0 {
                continue;
            }
            total_docs += weight;
            total_terms += entry.doc.len() * weight;
            for token in entry.doc.term_freqs.keys() {
                *doc_freqs.entry(token.clone()).or_insert(0usize) += weight;
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

impl PreparedMetadataQuery {
    fn new(query: &MetadataBm25Document, corpus: &MetadataBm25Corpus) -> Self {
        let terms = query_terms_from_tokens(&query.tokens);
        let self_score = bm25_score_terms(&terms, query, corpus);
        let denominator = if self_score > 0.0 { self_score } else { 1.0 };
        Self { terms, denominator }
    }

    fn has_term_overlap(&self, document: &MetadataBm25Document) -> bool {
        self.terms
            .iter()
            .any(|(token, _)| document.term_frequency(token) > 0)
    }
}

fn score_metadata_with_query(
    query: &PreparedMetadataQuery,
    right: &MetadataBm25Document,
    corpus: &MetadataBm25Corpus,
) -> f64 {
    if !query.has_term_overlap(right) {
        return 0.0;
    }
    (bm25_score_terms(&query.terms, right, corpus) / query.denominator).clamp(0.0, 1.0)
}

fn query_terms_from_tokens(query_tokens: &[String]) -> Vec<(String, usize)> {
    let mut query_terms = HashMap::<String, usize>::new();
    for token in query_tokens {
        *query_terms.entry(token.clone()).or_insert(0) += 1;
    }
    let mut query_terms = query_terms.into_iter().collect::<Vec<_>>();
    query_terms.sort_by(|left, right| left.0.cmp(&right.0));
    query_terms
}

fn bm25_score_terms(
    query_terms: &[(String, usize)],
    doc: &MetadataBm25Document,
    corpus: &MetadataBm25Corpus,
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
            let tf = doc.term_frequency(token) as f64;
            if tf == 0.0 {
                return 0.0;
            }
            let df = *corpus.doc_freqs.get(token).unwrap_or(&0) as f64;
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
            flatten_metadata(&value, &mut parts);
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

fn flatten_metadata(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key = key.to_lowercase();
                if matches!(
                    key.as_str(),
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
                ) {
                    flatten_metadata(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                flatten_metadata(item, parts);
            }
        }
        Value::String(text) if !text.trim().is_empty() => parts.push(text.trim().to_string()),
        _ => {}
    }
}

fn normalize_metadata_text(raw: &str) -> String {
    raw.to_lowercase()
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

    fn metadata_doc_entry(text: &str) -> MetadataDocEntry {
        MetadataDocEntry {
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
    fn metadata_doc_pair_hits_are_collected_for_left_range() {
        let docs = vec![
            metadata_doc_entry("gold dragon"),
            metadata_doc_entry("dragon gold"),
            metadata_doc_entry("silver cat"),
            metadata_doc_entry("gold dragon"),
        ];
        let corpus = MetadataBm25Corpus::from_doc_entries(&docs);
        let queries = docs
            .iter()
            .map(|entry| PreparedMetadataQuery::new(&entry.doc, &corpus))
            .collect::<Vec<_>>();
        let postings = metadata_token_postings(&docs);

        let hits =
            collect_metadata_doc_pair_hits_for_left_range(1..3, &docs, &queries, &corpus, &postings);

        assert_eq!(hits, vec![(1, 3)]);
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
