use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::analysis::scoring::{
    metadata_document_from_json, metadata_is_dedup_eligible, metadata_recall_document,
    MetadataBm25Corpus, MetadataBm25Document,
};
use crate::models::{
    ContractDuplicateRecord, ContractNameRecord, ContractSignal, DatabaseNftRecord,
    DatabaseSnapshot, SeedNft,
};
use crate::normalize::{normalize_name, normalize_url};

use super::{
    METADATA_SIMHASH_BAND_BITS, METADATA_SIMHASH_BAND_VALUES, METADATA_SKETCH_ANCHOR_COUNT,
    METADATA_SKETCH_HIGH_FREQ_DIVISOR, METADATA_SKETCH_HIGH_FREQ_MIN_DOCS,
};

#[derive(Clone)]
pub(super) struct RecallRow {
    pub(super) feature_rowid: i64,
    pub(super) contract_address: String,
    pub(super) token_id: String,
    pub(super) token_uri_norm: String,
    pub(super) image_uri_norm: String,
    pub(super) name_norm: String,
    pub(super) metadata_recall_match: bool,
}

pub(super) struct SeedRecallProfile {
    pub(super) seed_address: String,
    pub(super) seed_contracts: HashSet<String>,
    pub(super) seed_token_ids: HashSet<String>,
    pub(super) exact_token_keys: HashSet<String>,
    pub(super) exact_image_keys: HashSet<String>,
    pub(super) seed_name_norms: Vec<String>,
    pub(super) seed_metadata_doc: Option<MetadataBm25Document>,
}

#[derive(Default)]
pub(super) struct SnapshotAccumulator {
    pub(super) per_contract_counts: HashMap<String, usize>,
    pub(super) nft_rows: Vec<DatabaseNftRecord>,
    pub(super) selected_rowids: HashMap<i64, usize>,
    pub(super) duplicate_rows_by_contract: HashMap<String, ContractDuplicateRecord>,
    pub(super) seen_contract_name_pairs: BTreeSet<(String, String)>,
    pub(super) seen_feature_rowids: HashSet<i64>,
    pub(super) contract_names: Vec<ContractNameRecord>,
    pub(super) contract_signals_raw: BTreeMap<String, ContractSignal>,
}

pub(super) struct SeedRowMatch {
    pub(super) token_uri_match: bool,
    pub(super) image_uri_match: bool,
    pub(super) name_prefix_match: bool,
    pub(super) metadata_recall_match: bool,
}

pub(super) struct SelectedRecallRow {
    pub(super) seed_index: usize,
    pub(super) row_index: usize,
    pub(super) row_match: SeedRowMatch,
}

pub(super) struct PendingMetadataRecallRow {
    pub(super) seed_index: usize,
    pub(super) row: RecallRow,
    pub(super) row_match: SeedRowMatch,
}

pub(super) struct SeedProfileIndex {
    pub(super) token_uri: HashMap<String, Vec<usize>>,
    pub(super) image_uri: HashMap<String, Vec<usize>>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct MetadataSketch {
    pub(super) simhash: u64,
    pub(super) anchors: Vec<String>,
}

#[derive(Clone)]
pub(super) struct MetadataRecallCandidate {
    pub(super) row: RecallRow,
    pub(super) doc: MetadataBm25Document,
    pub(super) sketch: MetadataSketch,
}

#[derive(Default)]
pub(super) struct MetadataSourceIndex {
    pub(super) anchor_indices: HashMap<String, Vec<usize>>,
    pub(super) simhash_band_indices: Vec<Vec<usize>>,
}

pub(super) struct MetadataRecallIndex {
    pub(super) candidates: Vec<MetadataRecallCandidate>,
    pub(super) corpus: MetadataBm25Corpus,
    pub(super) doc_freqs: HashMap<String, usize>,
    pub(super) source_index: MetadataSourceIndex,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PreparedRecallState {
    pub(super) ready: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct FeatureChainStats {
    pub(super) row_count: i64,
    pub(super) max_feature_rowid: i64,
    pub(super) fingerprint: String,
}

pub(super) fn seed_metadata_representative_doc(
    seed_nfts: &[SeedNft],
) -> Option<MetadataBm25Document> {
    seed_nfts.first().and_then(|item| {
        let doc = metadata_recall_document(&item.metadata_json);
        MetadataBm25Document::from_text(&doc)
    })
}

impl SeedRecallProfile {
    pub(super) fn new(seed_address: String, seed_nfts: &[SeedNft]) -> Self {
        Self {
            seed_address,
            seed_contracts: seed_nfts
                .iter()
                .map(|item| item.contract_address.to_lowercase())
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
            seed_name_norms: seed_nfts
                .iter()
                .map(|item| normalize_name(&item.name))
                .filter(|value| !value.is_empty())
                .collect(),
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
        self.selected_rowids
            .insert(row.feature_rowid, self.nft_rows.len());

        record.metadata_recall_checked = true;
        record.metadata_recall_match = row_match.metadata_recall_match;

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

    pub(super) fn mark_selected_metadata_recall(&mut self, row: &RecallRow) -> bool {
        let Some(index) = self.selected_rowids.get(&row.feature_rowid).copied() else {
            return false;
        };
        let Some(record) = self.nft_rows.get_mut(index) else {
            return false;
        };
        record.metadata_recall_checked = true;
        record.metadata_recall_match = true;
        let record = record.clone();

        if let Some(signal) = self.contract_signals_raw.get_mut(&record.contract_address) {
            signal.keyword_match = true;
        }
        if let Some(entry) = self
            .duplicate_rows_by_contract
            .get_mut(&record.contract_address)
        {
            entry.metadata_recall_checked = true;
            entry.metadata_recall_match = true;
            for token_row in &mut entry.metadata_token_rows {
                if token_row.token_id == record.token_id {
                    token_row.metadata_recall_checked = true;
                    token_row.metadata_recall_match = true;
                }
            }
            if !entry.representative.metadata_recall_match
                || record.token_id < entry.representative.token_id
            {
                entry.representative = record;
            }
        }
        true
    }

    pub(super) fn finish(self) -> DatabaseSnapshot {
        let mut duplicate_contract_rows: Vec<_> =
            self.duplicate_rows_by_contract.into_values().collect();
        for row in &mut duplicate_contract_rows {
            row.metadata_token_rows
                .sort_by(|left, right| left.token_id.cmp(&right.token_id));
        }
        duplicate_contract_rows
            .sort_by(|left, right| left.contract_address.cmp(&right.contract_address));

        DatabaseSnapshot {
            nft_rows: self.nft_rows,
            duplicate_contract_rows,
            contract_names: self.contract_names,
            contract_signals: self.contract_signals_raw,
        }
    }
}

impl SeedProfileIndex {
    pub(super) fn new(profiles: &[SeedRecallProfile]) -> Self {
        let mut index = Self {
            token_uri: HashMap::new(),
            image_uri: HashMap::new(),
        };
        for (profile_index, profile) in profiles.iter().enumerate() {
            Self::insert_values(
                &mut index.token_uri,
                &profile.exact_token_keys,
                profile_index,
            );
            Self::insert_values(
                &mut index.image_uri,
                &profile.exact_image_keys,
                profile_index,
            );
        }
        index
    }

    pub(super) fn insert_values(
        target: &mut HashMap<String, Vec<usize>>,
        values: &HashSet<String>,
        profile_index: usize,
    ) {
        for value in values {
            if value.is_empty() {
                continue;
            }
            target.entry(value.clone()).or_default().push(profile_index);
        }
    }

    pub(super) fn append_matching_profiles(
        target: &mut Vec<usize>,
        source: &HashMap<String, Vec<usize>>,
        value: &str,
    ) {
        if value.is_empty() {
            return;
        }
        let Some(profile_indices) = source.get(value) else {
            return;
        };
        for profile_index in profile_indices {
            if !target.contains(profile_index) {
                target.push(*profile_index);
            }
        }
    }

    pub(super) fn strong_match_profiles(&self, row: &RecallRow) -> Vec<usize> {
        let mut matches = Vec::new();
        Self::append_matching_profiles(&mut matches, &self.token_uri, &row.token_uri_norm);
        Self::append_matching_profiles(&mut matches, &self.image_uri, &row.image_uri_norm);
        matches
    }
}

pub(super) fn metadata_token_idf(total_docs: usize, doc_freq: usize) -> f64 {
    (((total_docs + 1) as f64) / ((doc_freq + 1) as f64)).ln() + 1.0
}

pub(super) fn metadata_token_is_high_frequency(total_docs: usize, doc_freq: usize) -> bool {
    doc_freq >= METADATA_SKETCH_HIGH_FREQ_MIN_DOCS
        && doc_freq.saturating_mul(METADATA_SKETCH_HIGH_FREQ_DIVISOR) > total_docs
}

pub(super) fn stable_token_hash(token: &str) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325u64;
    for byte in token.as_bytes() {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(super) fn metadata_simhash_from_weights(weights: [f64; 64]) -> u64 {
    let mut simhash = 0u64;
    for (bit, weight) in weights.into_iter().enumerate() {
        if weight >= 0.0 {
            simhash |= 1u64 << bit;
        }
    }
    simhash
}

pub(super) fn compare_metadata_anchor_quality(
    left: &(String, f64),
    right: &(String, f64),
) -> std::cmp::Ordering {
    left.1
        .partial_cmp(&right.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| right.0.cmp(&left.0))
}

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

pub(super) fn metadata_sketch_from_document(
    document: &MetadataBm25Document,
    total_docs: usize,
    doc_freqs: &HashMap<String, usize>,
) -> MetadataSketch {
    let mut weights = [0.0f64; 64];
    let mut anchors = Vec::<(String, f64)>::new();
    let unique_tokens = document.tokens().iter().collect::<BTreeSet<_>>();
    for token in unique_tokens {
        let df = doc_freqs.get(token).copied().unwrap_or(0);
        let idf = metadata_token_idf(total_docs, df);
        let token_hash = stable_token_hash(token);
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
    MetadataSketch {
        simhash: metadata_simhash_from_weights(weights),
        anchors,
    }
}

pub(super) fn metadata_simhash_band_key(band_index: usize, band_value: u8) -> usize {
    band_index * METADATA_SIMHASH_BAND_VALUES + band_value as usize
}

pub(super) fn metadata_simhash_band_value(simhash: u64, band_index: usize) -> u8 {
    ((simhash >> (band_index * METADATA_SIMHASH_BAND_BITS)) & 0xff) as u8
}

pub(super) fn sorted_strings_intersect(left: &[String], right: &[String]) -> bool {
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
    if !seed.anchors.is_empty() && sorted_strings_intersect(&seed.anchors, &candidate.anchors) {
        return true;
    }
    (seed.simhash ^ candidate.simhash).count_ones() <= hamming_threshold
}

pub(super) fn metadata_seed_doc_for_index(
    seed_doc: &MetadataBm25Document,
    metadata_index: &MetadataRecallIndex,
) -> Option<MetadataBm25Document> {
    let tokens = seed_doc
        .tokens()
        .iter()
        .filter(|token| metadata_index.doc_freqs.contains_key(*token))
        .cloned()
        .collect::<Vec<_>>();
    MetadataBm25Document::from_tokens(tokens)
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
