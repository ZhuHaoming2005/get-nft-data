use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[cfg(test)]
use crate::analysis::scoring::stable_metadata_token_hash;
use crate::analysis::scoring::{
    metadata_document_from_json, metadata_is_dedup_eligible, metadata_recall_document,
    CompactMetadataBm25Corpus, CompactMetadataBm25Document, MetadataBm25Document,
    PreparedNameQuery,
};
use crate::models::{
    normalize_chain_identity, ContractDuplicateRecord, ContractNameRecord, ContractSignal,
    DatabaseNftRecord, DatabaseSnapshot, SeedNft,
};
use crate::normalize::{normalize_name, normalize_url};

#[cfg(test)]
use super::{
    METADATA_SKETCH_ANCHOR_COUNT, METADATA_SKETCH_HIGH_FREQ_DIVISOR,
    METADATA_SKETCH_HIGH_FREQ_MIN_DOCS,
};

#[derive(Clone)]
pub(super) struct RecallRow {
    pub(super) feature_rowid: i64,
    pub(super) contract_address: String,
    pub(super) token_id: String,
    pub(super) token_uri_norm: String,
    pub(super) image_uri_norm: String,
    pub(super) name_norm: String,
}

pub(super) struct SeedRecallProfile {
    pub(super) seed_address: String,
    pub(super) seed_contracts: HashSet<String>,
    pub(super) seed_token_ids: HashSet<String>,
    pub(super) exact_token_keys: HashSet<String>,
    pub(super) exact_image_keys: HashSet<String>,
    pub(super) seed_name_norms: Vec<String>,
    pub(super) seed_name_queries: Vec<PreparedNameQuery>,
    pub(super) seed_metadata_doc: Option<MetadataBm25Document>,
}

#[derive(Default)]
pub(super) struct SnapshotAccumulator {
    pub(super) per_contract_counts: HashMap<String, usize>,
    pub(super) nft_rows: Vec<DatabaseNftRecord>,
    pub(super) duplicate_rows_by_contract: HashMap<String, ContractDuplicateRecord>,
    pub(super) seen_contract_name_pairs: BTreeSet<(String, String)>,
    pub(super) seen_feature_rowids: HashSet<i64>,
    pub(super) contract_names: Vec<ContractNameRecord>,
    pub(super) contract_signals_raw: BTreeMap<String, ContractSignal>,
    pub(super) estimated_owned_bytes: usize,
}

pub(super) struct SeedRowMatch {
    pub(super) token_uri_match: bool,
    pub(super) image_uri_match: bool,
    pub(super) name_prefix_match: bool,
    pub(super) metadata_recall_match: bool,
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(super) struct MetadataSketch {
    pub(super) simhash: u64,
    pub(super) anchors: Vec<u32>,
}

#[derive(Clone)]
pub(super) struct MetadataRecallCandidate {
    pub(super) feature_rowid: i64,
    pub(super) contract_address: String,
}

pub(super) struct MetadataRecallIndex {
    pub(super) candidates: Vec<MetadataRecallCandidate>,
    pub(super) compact_corpus: CompactMetadataBm25Corpus,
    pub(super) compact_documents: Vec<CompactMetadataBm25Document>,
    pub(super) term_postings: Vec<Vec<u32>>,
}

pub(super) struct MetadataCandidateScratch {
    seen_generations: Vec<u16>,
    generation: u16,
    pub(super) candidate_indices: Vec<u32>,
}

impl MetadataCandidateScratch {
    pub(super) fn new(candidate_count: usize) -> Self {
        Self {
            seen_generations: vec![0; candidate_count],
            generation: 0,
            candidate_indices: Vec::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.candidate_indices.clear();
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen_generations.fill(0);
            self.generation = 1;
        }
    }

    pub(super) fn insert(&mut self, candidate_index: usize) -> bool {
        let Some(slot) = self.seen_generations.get_mut(candidate_index) else {
            return false;
        };
        if *slot == self.generation {
            return false;
        }
        *slot = self.generation;
        true
    }
}

impl MetadataRecallIndex {
    pub(super) fn memory_bytes(&self) -> usize {
        let candidate_bytes = self
            .candidates
            .iter()
            .map(|candidate| {
                std::mem::size_of::<MetadataRecallCandidate>()
                    .saturating_add(candidate.contract_address.capacity())
            })
            .sum::<usize>();
        let document_bytes = self
            .compact_documents
            .iter()
            .map(CompactMetadataBm25Document::memory_bytes)
            .sum::<usize>();
        let posting_bytes = self
            .term_postings
            .iter()
            .map(|postings| {
                std::mem::size_of::<Vec<u32>>().saturating_add(
                    postings
                        .capacity()
                        .saturating_mul(std::mem::size_of::<u32>()),
                )
            })
            .sum::<usize>();
        std::mem::size_of::<Self>()
            .saturating_add(candidate_bytes)
            .saturating_add(document_bytes)
            .saturating_add(self.compact_corpus.memory_bytes())
            .saturating_add(posting_bytes)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PreparedRecallState {
    pub(super) ready: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct FeatureGenerationState {
    pub(super) generation_id: String,
    pub(super) row_count: i64,
    pub(super) contract_count: i64,
}

pub(super) fn seed_metadata_representative_doc(
    seed_nfts: &[SeedNft],
) -> Option<MetadataBm25Document> {
    seed_nfts.iter().find_map(|item| {
        let doc = metadata_recall_document(&item.metadata_json);
        MetadataBm25Document::from_text(&doc)
    })
}

impl SeedRecallProfile {
    pub(super) fn new(seed_address: String, seed_nfts: &[SeedNft]) -> Self {
        let seed_name_norms = seed_nfts
            .iter()
            .map(|item| normalize_name(&item.name))
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        let seed_name_queries = seed_name_norms
            .iter()
            .map(|name| PreparedNameQuery::new(name))
            .collect();
        Self {
            seed_address,
            seed_contracts: seed_nfts
                .iter()
                .map(|item| normalize_chain_identity(&item.contract_address))
                .collect(),
            seed_token_ids: seed_nfts
                .iter()
                .map(|item| item.token_id.clone())
                .filter(|value| !value.is_empty())
                .collect(),
            exact_token_keys: seed_nfts
                .iter()
                .filter_map(|item| normalize_url(&item.token_uri))
                .collect(),
            exact_image_keys: seed_nfts
                .iter()
                .filter_map(|item| normalize_url(&item.image_uri))
                .collect(),
            seed_name_norms,
            seed_name_queries,
            seed_metadata_doc: seed_metadata_representative_doc(seed_nfts),
        }
    }

    pub(super) fn has_strong_recall_keys(&self) -> bool {
        !self.exact_token_keys.is_empty()
            || !self.exact_image_keys.is_empty()
            || !self.seed_name_norms.is_empty()
    }
}

impl SnapshotAccumulator {
    fn record_owned_bytes(record: &DatabaseNftRecord) -> usize {
        std::mem::size_of::<DatabaseNftRecord>()
            .saturating_add(record.contract_address.capacity())
            .saturating_add(record.token_id.capacity())
            .saturating_add(record.token_uri.capacity())
            .saturating_add(record.image_uri.capacity())
            .saturating_add(record.name.capacity())
            .saturating_add(record.symbol.capacity())
            .saturating_add(record.metadata_json.capacity())
    }

    pub(super) fn estimated_memory_bytes(&self) -> usize {
        self.estimated_owned_bytes
            .saturating_add(
                self.nft_rows
                    .capacity()
                    .saturating_mul(std::mem::size_of::<DatabaseNftRecord>()),
            )
            .saturating_add(
                self.seen_feature_rowids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<i64>().saturating_mul(3)),
            )
    }

    pub(super) fn push_recall_row(
        &mut self,
        profile: &SeedRecallProfile,
        row: &RecallRow,
        mut record: DatabaseNftRecord,
        row_match: &SeedRowMatch,
        max_tokens_per_contract: usize,
    ) {
        if profile.seed_contracts.contains(&row.contract_address) {
            return;
        }

        if self.seen_feature_rowids.contains(&row.feature_rowid) {
            return;
        }

        let entry = self
            .per_contract_counts
            .entry(row.contract_address.clone())
            .or_default();
        if max_tokens_per_contract > 0 && *entry >= max_tokens_per_contract {
            return;
        }
        *entry += 1;
        self.seen_feature_rowids.insert(row.feature_rowid);

        record.metadata_recall_checked = true;
        record.metadata_recall_match = row_match.metadata_recall_match;
        let record_bytes = Self::record_owned_bytes(&record);
        let normalized_bytes = row
            .contract_address
            .capacity()
            .saturating_add(row.token_id.capacity())
            .saturating_add(row.token_uri_norm.capacity())
            .saturating_add(row.image_uri_norm.capacity())
            .saturating_add(row.name_norm.capacity());
        // The selected record is retained in the snapshot and cloned into
        // duplicate-contract projections and metadata-token evidence. This
        // deliberately overestimates allocator overhead rather than allowing
        // a seed to cross its configured memory envelope silently.
        self.estimated_owned_bytes = self
            .estimated_owned_bytes
            .saturating_add(record_bytes.saturating_mul(4))
            .saturating_add(normalized_bytes.saturating_mul(3))
            .saturating_add(1024);

        let name_pair_is_new = !row.name_norm.is_empty()
            && self
                .seen_contract_name_pairs
                .insert((record.contract_address.clone(), row.name_norm.clone()));
        if name_pair_is_new {
            self.contract_names.push(ContractNameRecord {
                contract_address: record.contract_address.clone(),
                name_norm: row.name_norm.clone(),
            });
        }

        let signal = self
            .contract_signals_raw
            .entry(record.contract_address.clone())
            .or_insert_with(|| ContractSignal {
                contract_address: record.contract_address.clone(),
                ..ContractSignal::default()
            });
        signal.token_count += 1;
        if row_match.token_uri_match {
            signal.uri_match_count += 1;
        }
        if row_match.image_uri_match {
            signal.image_match_count += 1;
        }
        if row_match.name_prefix_match {
            signal.name_prefix_match = true;
        }
        if row_match.metadata_recall_match {
            signal.keyword_match = true;
        }

        update_duplicate_contract_row(
            &mut self.duplicate_rows_by_contract,
            &record,
            row_match.token_uri_match,
            row_match.image_uri_match,
            &row.name_norm,
            name_pair_is_new,
            row_match.metadata_recall_match,
        );
        self.nft_rows.push(record);
    }

    pub(super) fn finish(self) -> DatabaseSnapshot {
        let mut nft_rows = self.nft_rows;
        nft_rows.sort_by(|left, right| {
            (&left.contract_address, &left.token_id)
                .cmp(&(&right.contract_address, &right.token_id))
        });
        let mut contract_names = self.contract_names;
        contract_names.sort_by(|left, right| {
            (&left.contract_address, &left.name_norm)
                .cmp(&(&right.contract_address, &right.name_norm))
        });
        let mut duplicate_contract_rows: Vec<_> =
            self.duplicate_rows_by_contract.into_values().collect();
        for row in &mut duplicate_contract_rows {
            row.metadata_token_rows
                .sort_by(|left, right| left.token_id.cmp(&right.token_id));
        }
        duplicate_contract_rows
            .sort_by(|left, right| left.contract_address.cmp(&right.contract_address));

        DatabaseSnapshot {
            nft_rows,
            duplicate_contract_rows,
            contract_names,
            contract_signals: self.contract_signals_raw,
        }
    }
}

#[cfg(test)]
pub(super) fn metadata_token_idf(total_docs: usize, doc_freq: usize) -> f64 {
    (((total_docs + 1) as f64) / ((doc_freq + 1) as f64)).ln() + 1.0
}

#[cfg(test)]
pub(super) fn metadata_token_is_high_frequency(total_docs: usize, doc_freq: usize) -> bool {
    doc_freq >= METADATA_SKETCH_HIGH_FREQ_MIN_DOCS
        && doc_freq.saturating_mul(METADATA_SKETCH_HIGH_FREQ_DIVISOR) > total_docs
}

#[cfg(test)]
pub(super) fn metadata_simhash_from_weights(weights: [f64; 64]) -> u64 {
    let mut simhash = 0u64;
    for (bit, weight) in weights.into_iter().enumerate() {
        if weight >= 0.0 {
            simhash |= 1u64 << bit;
        }
    }
    simhash
}

#[cfg(test)]
pub(super) fn compare_metadata_anchor_quality(
    left: &(String, f64),
    right: &(String, f64),
) -> std::cmp::Ordering {
    left.1
        .partial_cmp(&right.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| right.0.cmp(&left.0))
}

#[cfg(test)]
pub(super) fn push_metadata_anchor_candidate(
    anchors: &mut Vec<(String, f64)>,
    candidate: (String, f64),
) {
    if anchors.len() < METADATA_SKETCH_ANCHOR_COUNT {
        anchors.push(candidate);
        return;
    }

    let Some((worst_index, worst_anchor)) = anchors
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| compare_metadata_anchor_quality(left, right))
    else {
        return;
    };
    if compare_metadata_anchor_quality(&candidate, worst_anchor).is_gt() {
        anchors[worst_index] = candidate;
    }
}

#[cfg(test)]
pub(super) fn metadata_sketch_from_document(
    document: &MetadataBm25Document,
    total_docs: usize,
    doc_freqs: &HashMap<String, usize>,
) -> (u64, Vec<String>) {
    metadata_sketch_from_document_with_lookup(document, total_docs, |token| {
        doc_freqs.get(token).copied().unwrap_or(0)
    })
}

#[cfg(test)]
pub(super) fn metadata_sketch_from_compact_corpus(
    document: &MetadataBm25Document,
    corpus: &CompactMetadataBm25Corpus,
) -> MetadataSketch {
    metadata_sketch_from_compact_document(&corpus.compact_document(document), corpus)
}

#[cfg(test)]
fn metadata_sketch_from_document_with_lookup(
    document: &MetadataBm25Document,
    total_docs: usize,
    mut doc_freq: impl FnMut(&str) -> usize,
) -> (u64, Vec<String>) {
    let mut weights = [0.0f64; 64];
    let mut anchors = Vec::<(String, f64)>::new();
    let unique_tokens = document.tokens().iter().collect::<BTreeSet<_>>();
    for token in unique_tokens {
        let df = doc_freq(token);
        let idf = metadata_token_idf(total_docs, df);
        let token_hash = stable_metadata_token_hash(token);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if ((token_hash >> bit) & 1) == 1 {
                *weight += idf;
            } else {
                *weight -= idf;
            }
        }
        if metadata_token_is_high_frequency(total_docs, df) {
            continue;
        }
        push_metadata_anchor_candidate(&mut anchors, ((*token).clone(), idf));
    }
    let mut anchors = anchors
        .into_iter()
        .map(|(token, _)| token)
        .collect::<Vec<_>>();
    anchors.sort();
    (metadata_simhash_from_weights(weights), anchors)
}

#[cfg(test)]
pub(super) fn metadata_sketch_from_compact_document(
    document: &CompactMetadataBm25Document,
    corpus: &CompactMetadataBm25Corpus,
) -> MetadataSketch {
    let mut weights = [0.0f64; 64];
    let mut anchors = Vec::<(u32, u32, f64)>::new();
    for (token_id, _) in document.terms() {
        let df = corpus.token_doc_freq_by_id(*token_id);
        let idf = metadata_token_idf(corpus.total_docs(), df);
        let token_hash = corpus.token_hash(*token_id);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if ((token_hash >> bit) & 1) == 1 {
                *weight += idf;
            } else {
                *weight -= idf;
            }
        }
        if metadata_token_is_high_frequency(corpus.total_docs(), df) {
            continue;
        }
        let candidate = (*token_id, corpus.token_lexical_rank(*token_id), idf);
        if anchors.len() < METADATA_SKETCH_ANCHOR_COUNT {
            anchors.push(candidate);
            continue;
        }
        let worst_index = anchors
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.2
                    .partial_cmp(&right.2)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| right.1.cmp(&left.1))
            })
            .map(|(index, _)| index)
            .expect("non-empty metadata anchor heap");
        let worst = anchors[worst_index];
        let candidate_quality = candidate
            .2
            .partial_cmp(&worst.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| worst.1.cmp(&candidate.1));
        if candidate_quality.is_gt() {
            anchors[worst_index] = candidate;
        }
    }
    let mut anchors = anchors
        .into_iter()
        .map(|(token_id, _, _)| token_id)
        .collect::<Vec<_>>();
    anchors.sort_unstable();
    MetadataSketch {
        simhash: metadata_simhash_from_weights(weights),
        anchors,
    }
}

#[cfg(test)]
pub(super) fn sorted_anchor_ids_intersect(left: &[u32], right: &[u32]) -> bool {
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

#[cfg(test)]
pub(super) fn metadata_sketch_source_match(
    seed: &MetadataSketch,
    candidate: &MetadataSketch,
    hamming_threshold: u32,
) -> bool {
    if (seed.simhash == 0 && seed.anchors.is_empty())
        || (candidate.simhash == 0 && candidate.anchors.is_empty())
    {
        return false;
    }
    if !seed.anchors.is_empty() && sorted_anchor_ids_intersect(&seed.anchors, &candidate.anchors) {
        return true;
    }
    (seed.simhash ^ candidate.simhash).count_ones() <= hamming_threshold
}

pub(super) fn update_duplicate_contract_row(
    rows_by_contract: &mut HashMap<String, ContractDuplicateRecord>,
    record: &DatabaseNftRecord,
    token_uri_match: bool,
    image_uri_match: bool,
    name_norm: &str,
    name_pair_is_new: bool,
    metadata_recall_match: bool,
) {
    let entry = rows_by_contract
        .entry(record.contract_address.clone())
        .or_insert_with(|| ContractDuplicateRecord {
            contract_address: record.contract_address.clone(),
            representative: record.clone(),
            ..ContractDuplicateRecord::default()
        });

    entry.token_uri_match |= token_uri_match;
    entry.image_uri_match |= image_uri_match;
    if name_pair_is_new && !name_norm.is_empty() {
        entry.name_norms.push(name_norm.to_string());
    }
    push_metadata_token_row(entry, record);

    entry.metadata_recall_checked = true;
    entry.metadata_recall_match |= metadata_recall_match;
    let should_update_representative = metadata_recall_match
        && (!entry.representative.metadata_recall_match
            || record.token_id < entry.representative.token_id);
    if !should_update_representative {
        return;
    }
    entry.representative = record.clone();
}

pub(super) fn push_metadata_token_row(
    entry: &mut ContractDuplicateRecord,
    record: &DatabaseNftRecord,
) {
    if !metadata_is_dedup_eligible(&record.metadata_json) {
        return;
    }
    if metadata_document_from_json(&record.metadata_json).is_empty() {
        return;
    }
    if entry
        .metadata_token_rows
        .iter()
        .any(|row| row.token_id == record.token_id)
    {
        return;
    }
    entry.metadata_token_rows.push(record.clone());
}
