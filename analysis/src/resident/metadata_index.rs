use crate::model::{MetadataId, ProfileId, TermId};
use crate::resident::{FrozenBytePool, MetadataFeatureStore};
use ahash::AHashMap;
use rayon::prelude::*;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use unicode_normalization::UnicodeNormalization;

const ARBITRARY_PRECISION_NUMBER_TOKEN: &str = "$serde_json::private::Number";
type AnalyzedDocumentChunk = (Vec<PreparedDocument>, Vec<(TermId, u32)>, Vec<(u32, u32)>);

pub fn canonicalize_json(raw: &str) -> Option<String> {
    serde_json::to_string(&parse_normalized_json(raw)?).ok()
}

pub fn is_valid_metadata_json(raw: &str) -> bool {
    parse_normalized_json(raw)
        .is_some_and(|value| !matches!(value, Value::Object(ref map) if map.is_empty()))
}

fn parse_normalized_json(raw: &str) -> Option<Value> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > 64 * 1024
        || !matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'['))
    {
        return None;
    }
    let mut deserializer = serde_json::Deserializer::from_str(trimmed);
    let value = StrictValue::deserialize(&mut deserializer).ok()?.0;
    deserializer.end().ok()?;
    normalize_value(value)
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictVisitor)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<StrictValue>()? {
            values.push(value.0);
        }
        Ok(StrictValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = BTreeSet::new();
        let mut output = Map::new();
        let Some(first_key) = map.next_key::<String>()? else {
            return Ok(StrictValue(Value::Object(output)));
        };
        if first_key == ARBITRARY_PRECISION_NUMBER_TOKEN {
            let raw = map.next_value::<String>()?;
            if map.next_key::<String>()?.is_some() {
                return Err(de::Error::custom(
                    "invalid arbitrary-precision number representation",
                ));
            }
            return raw
                .parse::<Number>()
                .map(Value::Number)
                .map(StrictValue)
                .map_err(de::Error::custom);
        }
        keys.insert(first_key.clone());
        output.insert(first_key, map.next_value::<StrictValue>()?.0);
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key `{key}`"
                )));
            }
            output.insert(key, map.next_value::<StrictValue>()?.0);
        }
        Ok(StrictValue(Value::Object(output)))
    }
}

fn normalize_value(value: Value) -> Option<Value> {
    match value {
        Value::String(value) => Some(Value::String(normalize_string(&value))),
        Value::Number(number) => Some(Value::Number(normalize_number(number))),
        Value::Array(items) => Some(Value::Array(
            items
                .into_iter()
                .map(normalize_value)
                .collect::<Option<Vec<_>>>()?,
        )),
        Value::Object(map) => {
            let mut entries = Vec::with_capacity(map.len());
            let mut normalized_keys = BTreeSet::new();
            for (key, value) in map {
                let key = normalize_string(&key);
                if !normalized_keys.insert(key.clone()) {
                    return None;
                }
                entries.push((key, normalize_value(value)?));
            }
            if let Some((_, Value::Array(attributes))) =
                entries.iter_mut().find(|(key, _)| key == "attributes")
            {
                attributes.sort_by_key(attribute_key);
            }
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Some(Value::Object(entries.into_iter().collect()))
        }
        other => Some(other),
    }
}

fn normalize_number(number: Number) -> Number {
    if let Some(value) = number.as_i64() {
        return Number::from(value);
    }
    if let Some(value) = number.as_u64() {
        return Number::from(value);
    }
    let raw = number.to_string();
    canonical_decimal(&raw)
        .and_then(|value| value.parse::<Number>().ok())
        .unwrap_or(number)
}

fn canonical_decimal(raw: &str) -> Option<String> {
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |value| (true, value));
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => (
            &unsigned[..index],
            unsigned[index + 1..].parse::<i128>().ok()?,
        ),
        None => (unsigned, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(integer);
    digits.push_str(fraction);
    let first_nonzero = digits
        .find(|character| character != '0')
        .unwrap_or(digits.len());
    if first_nonzero == digits.len() {
        return Some("0".to_owned());
    }
    digits.drain(..first_nonzero);
    let trailing_zeros = digits.len() - digits.trim_end_matches('0').len();
    digits.truncate(digits.len() - trailing_zeros);
    let power = exponent
        .checked_sub(i128::try_from(fraction.len()).ok()?)?
        .checked_add(i128::try_from(trailing_zeros).ok()?)?;
    let mut output = String::new();
    if negative {
        output.push('-');
    }
    if (0..=64).contains(&power) {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n('0', usize::try_from(power).ok()?));
        return Some(output);
    }
    let decimal_position = i128::try_from(digits.len()).ok()?.checked_add(power)?;
    if decimal_position > 0 && decimal_position < i128::try_from(digits.len()).ok()? {
        let split = usize::try_from(decimal_position).ok()?;
        output.push_str(&digits[..split]);
        output.push('.');
        output.push_str(&digits[split..]);
        return Some(output);
    }
    if (-64..=0).contains(&decimal_position) {
        output.push_str("0.");
        output.extend(std::iter::repeat_n(
            '0',
            usize::try_from(-decimal_position).ok()?,
        ));
        output.push_str(&digits);
        return Some(output);
    }
    output.push(digits.as_bytes()[0] as char);
    if digits.len() > 1 {
        output.push('.');
        output.push_str(&digits[1..]);
    }
    let scientific_exponent = power.checked_add(i128::try_from(digits.len() - 1).ok()?)?;
    if scientific_exponent != 0 {
        output.push('e');
        output.push_str(&scientific_exponent.to_string());
    }
    Some(output)
}

fn attribute_key(value: &Value) -> (String, String) {
    match value {
        Value::Object(map) => (
            map.get("trait_type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            map.get("value").map(Value::to_string).unwrap_or_default(),
        ),
        _ => (String::new(), value.to_string()),
    }
}

fn normalize_string(input: &str) -> String {
    input
        .nfkc()
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Clone, Debug)]
pub struct PreparedDocument {
    term_start: u64,
    term_len: u32,
    histogram_start: u64,
    histogram_len: u16,
    pub length: u32,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct MetadataIndex {
    pub documents: Vec<PreparedDocument>,
    pub terms: Vec<(TermId, u32)>,
    pub frequency_histograms: Vec<(u32, u32)>,
    pub term_pool: FrozenBytePool,
    pub shards: Vec<MetadataShardIndex>,
    /// Global (all-shard) profile count per max-anchor term posting key, for
    /// every key that survived singleton pruning (see `prune_singleton_postings`).
    /// Lower counts are rarer and should be probed first in the BM25 wave.
    pub term_rarity: AHashMap<TermId, u32>,
    /// Global (all-shard) profile count per `(anchor_token_id, term)` posting
    /// key, for every key that survived singleton pruning.
    pub token_term_rarity: AHashMap<(u32, TermId), u32>,
}

#[derive(Clone, Debug, Default)]
pub struct MetadataShardIndex {
    pub profiles: Vec<ProfileId>,
    pub exact_postings: AHashMap<[u8; 32], Vec<ProfileId>>,
    pub term_postings: AHashMap<TermId, Vec<ProfileId>>,
    pub token_term_postings: AHashMap<(u32, TermId), Vec<ProfileId>>,
}

#[derive(Clone, Debug, Default)]
pub struct PreparedMetadataQuery {
    pub exact_digests: Vec<[u8; 32]>,
    pub token_term_probes: Vec<(u32, TermId)>,
    pub term_probes: Vec<TermId>,
}

impl MetadataIndex {
    pub fn build(
        features: &MetadataFeatureStore,
        seed_documents: &[MetadataId],
        shard_count: usize,
    ) -> Self {
        Self::build_inner(features, seed_documents, shard_count, None, None)
    }

    pub fn build_numa(
        features: &MetadataFeatureStore,
        seed_documents: &[MetadataId],
        shard_count: usize,
        executor: &crate::pipeline::CpuExecutor,
    ) -> Self {
        Self::build_inner(features, seed_documents, shard_count, Some(executor), None)
    }

    pub fn build_numa_with_progress(
        features: &MetadataFeatureStore,
        seed_documents: &[MetadataId],
        shard_count: usize,
        executor: &crate::pipeline::CpuExecutor,
        progress: &crate::progress::Progress,
    ) -> Self {
        Self::build_inner(
            features,
            seed_documents,
            shard_count,
            Some(executor),
            Some(progress),
        )
    }

    fn build_inner(
        features: &MetadataFeatureStore,
        seed_documents: &[MetadataId],
        shard_count: usize,
        executor: Option<&crate::pipeline::CpuExecutor>,
        progress: Option<&crate::progress::Progress>,
    ) -> Self {
        let mut seed_term_bytes = seed_documents
            .iter()
            .flat_map(|document| tokenize(features.documents.get(document.0)))
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let mut term_interner = crate::resident::ByteInterner::default();
        let seed_term_ids = seed_term_bytes
            .iter()
            .map(|term| (term.clone(), TermId(term_interner.intern(term))))
            .collect::<AHashMap<_, _>>();
        seed_term_bytes.clear();
        const DOCUMENT_CHUNK: usize = 4_096;
        let chunk_count = features.documents.len().div_ceil(DOCUMENT_CHUNK);
        let analyze_chunk = |chunk: usize, scratch: &mut DocumentScratch| {
            let start = chunk * DOCUMENT_CHUNK;
            let end = (start + DOCUMENT_CHUNK).min(features.documents.len());
            let mut documents = Vec::with_capacity(end - start);
            let mut terms = Vec::new();
            let mut histograms = Vec::new();
            for document in start..end {
                let canonical = features.documents.get(document as u32);
                analyze_document_tokens(canonical, &seed_term_ids, scratch);
                documents.push(PreparedDocument {
                    term_start: terms.len() as u64,
                    term_len: scratch.intersections.len() as u32,
                    histogram_start: histograms.len() as u64,
                    histogram_len: scratch.histogram.len() as u16,
                    length: scratch.frequencies.iter().copied().sum(),
                    digest: Sha256::digest(canonical.as_bytes()).into(),
                });
                terms.extend_from_slice(&scratch.intersections);
                histograms.extend_from_slice(&scratch.histogram);
            }
            (documents, terms, histograms)
        };
        let chunks: Vec<AnalyzedDocumentChunk> = if let Some(executor) = executor {
            let mut chunks = executor
                .install_on_all(|lane, lane_count| {
                    (0..chunk_count)
                        .into_par_iter()
                        .filter(|chunk| chunk % lane_count == lane)
                        .map_init(DocumentScratch::default, |scratch, chunk| {
                            let contents = analyze_chunk(chunk, scratch);
                            if let Some(progress) = progress {
                                progress.add_phase_completed(contents.0.len() as u64);
                            }
                            (chunk, contents)
                        })
                        .collect::<Vec<_>>()
                })
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            chunks.sort_unstable_by_key(|(chunk, _)| *chunk);
            chunks.into_iter().map(|(_, contents)| contents).collect()
        } else {
            (0..chunk_count)
                .into_par_iter()
                .map_init(DocumentScratch::default, |scratch, chunk| {
                    analyze_chunk(chunk, scratch)
                })
                .collect()
        };
        let document_count = chunks.iter().map(|chunk| chunk.0.len()).sum();
        let term_count = chunks.iter().map(|chunk| chunk.1.len()).sum();
        let histogram_count = chunks.iter().map(|chunk| chunk.2.len()).sum();
        let mut documents = Vec::with_capacity(document_count);
        let mut terms = Vec::with_capacity(term_count);
        let mut frequency_histograms = Vec::with_capacity(histogram_count);
        for (mut chunk_documents, mut chunk_terms, mut chunk_histograms) in chunks {
            let term_base = terms.len() as u64;
            let histogram_base = frequency_histograms.len() as u64;
            for document in &mut chunk_documents {
                document.term_start += term_base;
                document.histogram_start += histogram_base;
            }
            documents.append(&mut chunk_documents);
            terms.append(&mut chunk_terms);
            frequency_histograms.append(&mut chunk_histograms);
        }
        let term_pool = term_interner.freeze();
        let seed_digests = seed_documents
            .iter()
            .map(|document| documents[document.index()].digest)
            .collect::<BTreeSet<_>>();
        let mut profiles_by_shard = (0..shard_count).map(|_| Vec::new()).collect::<Vec<_>>();
        for profile_index in 0..features.profiles.len() {
            let profile_id = ProfileId(profile_index as u32);
            let owner = crate::model::owner_shard(profile_id.0, shard_count);
            profiles_by_shard[owner].push(profile_id);
        }
        let mut shards: Vec<MetadataShardIndex> = if let Some(executor) = executor {
            let mut shards = executor
                .install_on_all(|lane, lane_count| {
                    profiles_by_shard
                        .par_iter()
                        .enumerate()
                        .filter(|(shard, _)| shard % lane_count == lane)
                        .map(|(shard, profiles)| {
                            let completed = profiles.len() as u64;
                            let shard_index = build_metadata_shard(
                                profiles.clone(),
                                features,
                                &documents,
                                &terms,
                                &seed_digests,
                            );
                            if let Some(progress) = progress {
                                progress.add_phase_completed(completed);
                            }
                            (shard, shard_index)
                        })
                        .collect::<Vec<_>>()
                })
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            shards.sort_unstable_by_key(|(shard, _)| *shard);
            shards.into_iter().map(|(_, shard)| shard).collect()
        } else {
            profiles_by_shard
                .into_par_iter()
                .map(|profiles| {
                    build_metadata_shard(profiles, features, &documents, &terms, &seed_digests)
                })
                .collect()
        };
        let (term_rarity, token_term_rarity) = prune_singleton_postings(&mut shards);
        Self {
            documents,
            terms,
            frequency_histograms,
            term_pool,
            shards,
            term_rarity,
            token_term_rarity,
        }
    }

    pub fn document_terms(&self, document: MetadataId) -> &[(TermId, u32)] {
        term_slice(&self.terms, &self.documents[document.index()])
    }

    pub fn similarity(&self, left: MetadataId, right: MetadataId, threshold: f64) -> (bool, f64) {
        bm25_cosine(
            &self.documents[left.index()],
            self.document_terms(left),
            histogram_slice(&self.frequency_histograms, &self.documents[left.index()]),
            &self.documents[right.index()],
            self.document_terms(right),
            histogram_slice(&self.frequency_histograms, &self.documents[right.index()]),
            threshold,
        )
    }

    pub fn prepare_query(
        &self,
        features: &MetadataFeatureStore,
        seed_profile: Option<ProfileId>,
    ) -> PreparedMetadataQuery {
        let Some(seed_profile) = seed_profile else {
            return PreparedMetadataQuery::default();
        };
        let anchors = features.profile_anchors(seed_profile);
        let mut exact_digests = anchors
            .iter()
            .map(|anchor| self.documents[anchor.metadata_id.index()].digest)
            .collect::<Vec<_>>();
        exact_digests.sort_unstable();
        exact_digests.dedup();

        let mut token_term_probes = anchors
            .iter()
            .flat_map(|anchor| {
                self.document_terms(anchor.metadata_id)
                    .iter()
                    .map(move |&(term, _)| (anchor.token_id_id.0, term))
            })
            .filter_map(|key| {
                self.token_term_rarity
                    .get(&key)
                    .map(|&rarity| (rarity, key))
            })
            .collect::<Vec<_>>();
        token_term_probes.sort_unstable();
        token_term_probes.dedup_by_key(|(_, key)| *key);

        let mut term_probes = anchors
            .last()
            .into_iter()
            .flat_map(|anchor| self.document_terms(anchor.metadata_id))
            .filter_map(|&(term, _)| self.term_rarity.get(&term).map(|&rarity| (rarity, term)))
            .collect::<Vec<_>>();
        term_probes.sort_unstable();
        term_probes.dedup_by_key(|(_, term)| *term);

        PreparedMetadataQuery {
            exact_digests,
            token_term_probes: token_term_probes.into_iter().map(|(_, key)| key).collect(),
            term_probes: term_probes.into_iter().map(|(_, term)| term).collect(),
        }
    }

    pub fn posting_count(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .exact_postings
                    .values()
                    .chain(shard.term_postings.values())
                    .chain(shard.token_term_postings.values())
                    .map(|values| values.len() as u64)
                    .sum::<u64>()
            })
            .sum()
    }
}

fn build_metadata_shard(
    profiles: Vec<ProfileId>,
    features: &MetadataFeatureStore,
    documents: &[PreparedDocument],
    terms: &[(TermId, u32)],
    seed_digests: &BTreeSet<[u8; 32]>,
) -> MetadataShardIndex {
    let mut shard = MetadataShardIndex {
        profiles,
        ..Default::default()
    };
    for &profile_id in &shard.profiles {
        let anchors = features.profile_anchors(profile_id);
        let Some(max_anchor) = anchors.last() else {
            continue;
        };
        for anchor in anchors {
            let document = &documents[anchor.metadata_id.index()];
            if seed_digests.contains(&document.digest) {
                shard
                    .exact_postings
                    .entry(document.digest)
                    .or_default()
                    .push(profile_id);
            }
            for &(term, _) in term_slice(terms, document) {
                shard
                    .token_term_postings
                    .entry((anchor.token_id_id.0, term))
                    .or_default()
                    .push(profile_id);
            }
        }
        for &(term, _) in term_slice(terms, &documents[max_anchor.metadata_id.index()]) {
            shard
                .term_postings
                .entry(term)
                .or_default()
                .push(profile_id);
        }
    }
    for postings in shard.exact_postings.values_mut() {
        postings.sort_unstable();
        postings.dedup();
    }
    for postings in shard.term_postings.values_mut() {
        postings.sort_unstable();
        postings.dedup();
    }
    for postings in shard.token_term_postings.values_mut() {
        postings.sort_unstable();
        postings.dedup();
    }
    shard
}

/// A `term_postings`/`token_term_postings` key with only one profile
/// anywhere across all 128 shards can never connect two distinct profiles
/// (a match requires at least two), so it is safe -- and lossless per
/// REWRITE_ARCHITECTURE.md §5.8 -- to drop it entirely. We compute the true
/// global count from the already-built per-shard maps (no extra corpus
/// scan), drop singleton entries from every shard, and keep the surviving
/// counts so the BM25 wave can probe rarer keys first.
fn prune_singleton_postings(
    shards: &mut [MetadataShardIndex],
) -> (AHashMap<TermId, u32>, AHashMap<(u32, TermId), u32>) {
    let mut term_totals: AHashMap<TermId, u32> = AHashMap::new();
    let mut token_term_totals: AHashMap<(u32, TermId), u32> = AHashMap::new();
    for shard in shards.iter() {
        for (&term, postings) in &shard.term_postings {
            *term_totals.entry(term).or_insert(0) += postings.len() as u32;
        }
        for (&key, postings) in &shard.token_term_postings {
            *token_term_totals.entry(key).or_insert(0) += postings.len() as u32;
        }
    }
    for shard in shards.iter_mut() {
        shard
            .term_postings
            .retain(|term, _| term_totals.get(term).copied().unwrap_or(0) >= 2);
        shard
            .token_term_postings
            .retain(|key, _| token_term_totals.get(key).copied().unwrap_or(0) >= 2);
    }
    term_totals.retain(|_, count| *count >= 2);
    token_term_totals.retain(|_, count| *count >= 2);
    (term_totals, token_term_totals)
}

#[derive(Default)]
struct DocumentScratch {
    ranges: Vec<(usize, usize)>,
    intersections: Vec<(TermId, u32)>,
    frequencies: Vec<u32>,
    histogram: Vec<(u32, u32)>,
}

#[cfg(test)]
fn summarize_document(
    canonical: &str,
    seed_term_ids: &AHashMap<String, TermId>,
    scratch: &mut DocumentScratch,
) -> PreparedDocument {
    analyze_document_tokens(canonical, seed_term_ids, scratch);
    PreparedDocument {
        term_start: 0,
        term_len: scratch.intersections.len() as u32,
        histogram_start: 0,
        histogram_len: scratch.histogram.len() as u16,
        length: scratch.frequencies.iter().copied().sum(),
        digest: Sha256::digest(canonical.as_bytes()).into(),
    }
}

fn analyze_document_tokens(
    canonical: &str,
    seed_term_ids: &AHashMap<String, TermId>,
    scratch: &mut DocumentScratch,
) {
    token_ranges(canonical, &mut scratch.ranges);
    scratch.ranges.sort_unstable_by(|left, right| {
        canonical[left.0..left.1].cmp(&canonical[right.0..right.1])
    });
    scratch.intersections.clear();
    scratch.frequencies.clear();
    let mut start = 0;
    while start < scratch.ranges.len() {
        let token_range = scratch.ranges[start];
        let token = &canonical[token_range.0..token_range.1];
        let mut end = start + 1;
        while end < scratch.ranges.len() {
            let candidate = scratch.ranges[end];
            if &canonical[candidate.0..candidate.1] != token {
                break;
            }
            end += 1;
        }
        let frequency = (end - start) as u32;
        scratch.frequencies.push(frequency);
        if let Some(&term) = seed_term_ids.get(token) {
            scratch.intersections.push((term, frequency));
        }
        start = end;
    }
    scratch
        .intersections
        .sort_unstable_by_key(|(term, _)| *term);
    scratch.frequencies.sort_unstable();
    scratch.histogram.clear();
    let mut start = 0;
    while start < scratch.frequencies.len() {
        let frequency = scratch.frequencies[start];
        let mut end = start + 1;
        while end < scratch.frequencies.len() && scratch.frequencies[end] == frequency {
            end += 1;
        }
        scratch.histogram.push((frequency, (end - start) as u32));
        start = end;
    }
}

fn token_ranges(value: &str, output: &mut Vec<(usize, usize)>) {
    output.clear();
    let mut start = None;
    for (offset, character) in value.char_indices() {
        if character.is_alphanumeric() {
            start.get_or_insert(offset);
        } else if let Some(start) = start.take() {
            if offset - start >= 2 {
                output.push((start, offset));
            }
        }
    }
    if let Some(start) = start {
        if value.len() - start >= 2 {
            output.push((start, value.len()));
        }
    }
}

pub fn tokenize(canonical: &str) -> impl Iterator<Item = &str> {
    canonical
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.len() >= 2)
}

fn term_slice<'a>(terms: &'a [(TermId, u32)], document: &PreparedDocument) -> &'a [(TermId, u32)] {
    let start = document.term_start as usize;
    &terms[start..start + document.term_len as usize]
}

fn histogram_slice<'a>(
    histograms: &'a [(u32, u32)],
    document: &PreparedDocument,
) -> &'a [(u32, u32)] {
    let start = document.histogram_start as usize;
    &histograms[start..start + document.histogram_len as usize]
}

pub fn bm25_cosine(
    left: &PreparedDocument,
    left_terms: &[(TermId, u32)],
    left_histogram: &[(u32, u32)],
    right: &PreparedDocument,
    right_terms: &[(TermId, u32)],
    right_histogram: &[(u32, u32)],
    threshold: f64,
) -> (bool, f64) {
    if threshold <= 0.0 {
        return (true, 1.0);
    }
    const IDF_ONCE: f64 = std::f64::consts::LN_2;
    const IDF_SHARED: f64 = 0.182_321_556_793_954_6;
    const IDF_RATIO_SQUARED: f64 = (IDF_SHARED / IDF_ONCE) * (IDF_SHARED / IDF_ONCE);
    let average_length = (left.length as f64 + right.length as f64) / 2.0;
    let left_norm = length_norm(left.length, average_length);
    let right_norm = length_norm(right.length, average_length);
    let mut left_squared = histogram_norm(left_histogram, left_norm, IDF_ONCE);
    let mut right_squared = histogram_norm(right_histogram, right_norm, IDF_ONCE);
    let (mut left_pos, mut right_pos, mut shared_left, mut shared_right, mut dot) =
        (0, 0, 0.0, 0.0, 0.0);
    while left_pos < left_terms.len() && right_pos < right_terms.len() {
        match left_terms[left_pos].0.cmp(&right_terms[right_pos].0) {
            std::cmp::Ordering::Equal => {
                let left_weight = bm25_weight(left_terms[left_pos].1, left_norm, IDF_ONCE);
                let right_weight = bm25_weight(right_terms[right_pos].1, right_norm, IDF_ONCE);
                shared_left += left_weight * left_weight;
                shared_right += right_weight * right_weight;
                dot += left_weight * right_weight;
                left_pos += 1;
                right_pos += 1;
            }
            std::cmp::Ordering::Less => left_pos += 1,
            std::cmp::Ordering::Greater => right_pos += 1,
        }
    }
    if dot == 0.0 {
        return (false, 0.0);
    }
    let reduction = 1.0 - IDF_RATIO_SQUARED;
    left_squared = (left_squared - reduction * shared_left).max(0.0);
    right_squared = (right_squared - reduction * shared_right).max(0.0);
    if left_squared == 0.0 || right_squared == 0.0 {
        return (false, 0.0);
    }
    let score = IDF_RATIO_SQUARED * dot / (left_squared.sqrt() * right_squared.sqrt());
    (score + 1e-12 >= threshold, score)
}

fn length_norm(length: u32, average: f64) -> f64 {
    const K1: f64 = 1.2;
    const B: f64 = 0.75;
    if average == 0.0 {
        K1
    } else {
        K1 * (1.0 - B + B * length as f64 / average)
    }
}

fn bm25_weight(frequency: u32, norm: f64, idf: f64) -> f64 {
    const K1: f64 = 1.2;
    let frequency = frequency as f64;
    idf * frequency * (K1 + 1.0) / (frequency + norm)
}

fn histogram_norm(histogram: &[(u32, u32)], norm: f64, idf: f64) -> f64 {
    histogram
        .iter()
        .map(|&(frequency, count)| {
            let weight = bm25_weight(frequency, norm, idf);
            weight * weight * f64::from(count)
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContractId, MetadataAnchor, TokenIdId};
    use crate::resident::value_pools::ByteInterner;
    use crate::resident::MetadataProfile;
    use std::collections::BTreeMap;

    fn term_id_for(index: &MetadataIndex, document: MetadataId, text: &str) -> TermId {
        index
            .document_terms(document)
            .iter()
            .map(|&(term, _)| term)
            .find(|&term| index.term_pool.get(term.0) == text)
            .unwrap_or_else(|| panic!("term `{text}` not present in document {document:?}"))
    }

    /// A `(token_id, term)` or `term` posting key with only one profile
    /// anywhere in the corpus can never connect two profiles, so
    /// `MetadataIndex::build` must drop it from every shard while keeping
    /// keys that are actually shared by two or more profiles.
    #[test]
    fn singleton_postings_are_pruned_while_shared_postings_survive() {
        let mut anchor_interner = ByteInterner::default();
        let token_a = TokenIdId(anchor_interner.intern("1"));
        let token_b = TokenIdId(anchor_interner.intern("2"));
        let anchor_tokens = anchor_interner.freeze();

        let mut doc_interner = ByteInterner::default();
        let doc_seed = MetadataId(doc_interner.intern(r#"{"description":"alpha beta"}"#));
        let doc_shared = MetadataId(doc_interner.intern(r#"{"description":"alpha gamma"}"#));
        let doc_lonely = MetadataId(doc_interner.intern(r#"{"description":"alpha delta"}"#));
        let documents = doc_interner.freeze();

        let anchors = vec![
            MetadataAnchor {
                token_id_id: token_a,
                metadata_id: doc_seed,
            },
            MetadataAnchor {
                token_id_id: token_a,
                metadata_id: doc_shared,
            },
            MetadataAnchor {
                token_id_id: token_b,
                metadata_id: doc_lonely,
            },
        ];
        let profiles = vec![
            MetadataProfile {
                anchor_start: 0,
                anchor_len: 1,
                member_start: 0,
                member_len: 1,
            },
            MetadataProfile {
                anchor_start: 1,
                anchor_len: 1,
                member_start: 1,
                member_len: 1,
            },
            MetadataProfile {
                anchor_start: 2,
                anchor_len: 1,
                member_start: 2,
                member_len: 1,
            },
        ];
        let features = MetadataFeatureStore {
            anchor_tokens,
            documents,
            anchors,
            profile_members: vec![ContractId(0), ContractId(1), ContractId(2)],
            profiles,
            contract_profiles: vec![Some(ProfileId(0)), Some(ProfileId(1)), Some(ProfileId(2))],
        };

        let index = MetadataIndex::build(&features, &[doc_seed], 4);
        let alpha = term_id_for(&index, doc_seed, "alpha");
        let beta = term_id_for(&index, doc_seed, "beta");

        // "alpha" is shared by all three profiles: kept, at both granularities.
        assert!(index.term_rarity.contains_key(&alpha));
        assert!(index.token_term_rarity.contains_key(&(token_a.0, alpha)));
        // "beta" only occurs in the seed's own document: pruned everywhere.
        assert!(!index.term_rarity.contains_key(&beta));
        assert!(!index.token_term_rarity.contains_key(&(token_a.0, beta)));
        // "alpha" tagged with token_b's id is unique to profile 2: pruned,
        // even though the plain term "alpha" itself is common.
        assert!(!index.token_term_rarity.contains_key(&(token_b.0, alpha)));

        for shard in &index.shards {
            assert!(!shard.term_postings.contains_key(&beta));
            assert!(!shard.token_term_postings.contains_key(&(token_a.0, beta)));
            assert!(!shard.token_term_postings.contains_key(&(token_b.0, alpha)));
        }
        let alpha_term_postings_total: usize = index
            .shards
            .iter()
            .filter_map(|shard| shard.term_postings.get(&alpha))
            .map(Vec::len)
            .sum();
        assert_eq!(alpha_term_postings_total, 3);
    }

    #[test]
    fn strict_canonicalization_rejects_duplicates_and_sorts_attributes() {
        assert!(canonicalize_json(r#"{"a":1,"a":2}"#).is_none());
        assert_eq!(
            canonicalize_json(
                r#"{"attributes":[{"trait_type":"z","value":1},{"trait_type":"a","value":2}]}"#
            )
            .unwrap(),
            r#"{"attributes":[{"trait_type":"a","value":2},{"trait_type":"z","value":1}]}"#
        );
    }

    #[test]
    fn bm25_cosine_is_symmetric() {
        let left = PreparedDocument {
            term_start: 0,
            term_len: 2,
            histogram_start: 0,
            histogram_len: 2,
            length: 3,
            digest: [0; 32],
        };
        let right = PreparedDocument {
            term_start: 0,
            term_len: 2,
            histogram_start: 0,
            histogram_len: 1,
            length: 2,
            digest: [1; 32],
        };
        let left_terms = [(TermId(0), 2), (TermId(1), 1)];
        let right_terms = [(TermId(0), 1), (TermId(2), 1)];
        let left_histogram = [(1, 1), (2, 1)];
        let right_histogram = [(1, 2)];
        let first = bm25_cosine(
            &left,
            &left_terms,
            &left_histogram,
            &right,
            &right_terms,
            &right_histogram,
            0.6,
        )
        .1;
        let second = bm25_cosine(
            &right,
            &right_terms,
            &right_histogram,
            &left,
            &left_terms,
            &left_histogram,
            0.6,
        )
        .1;
        assert!((first - second).abs() < 1e-12);
    }

    #[test]
    fn seed_only_term_compression_matches_full_vector_oracle() {
        let vocabulary = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta", "lambda", "omega",
            "pixel", "dragon", "forest",
        ];
        let mut state = 0x4d59_5df4_d0f3_3173_u64;
        for case in 0..512 {
            let left = generated_document(&vocabulary, &mut state);
            let right = generated_document(&vocabulary, &mut state);
            let seed_term_ids = tokenize(&left)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .enumerate()
                .map(|(index, term)| (term.to_owned(), TermId(index as u32)))
                .collect::<AHashMap<_, _>>();
            let mut left_scratch = DocumentScratch::default();
            let mut right_scratch = DocumentScratch::default();
            let compressed_left = summarize_document(&left, &seed_term_ids, &mut left_scratch);
            let compressed_right = summarize_document(&right, &seed_term_ids, &mut right_scratch);
            analyze_document_tokens(&left, &seed_term_ids, &mut left_scratch);
            analyze_document_tokens(&right, &seed_term_ids, &mut right_scratch);
            let compressed = bm25_cosine(
                &compressed_left,
                &left_scratch.intersections,
                &left_scratch.histogram,
                &compressed_right,
                &right_scratch.intersections,
                &right_scratch.histogram,
                0.6,
            )
            .1;
            let full = full_vector_similarity(&left, &right);
            assert!(
                (compressed - full).abs() < 1e-12,
                "case {case} diverged: compressed={compressed}, full={full}, left={left:?}, right={right:?}"
            );
        }
    }

    fn generated_document(vocabulary: &[&str], state: &mut u64) -> String {
        let count = 1 + usize::try_from(next_u32(state) % 24).unwrap();
        (0..count)
            .map(|_| {
                let index = usize::try_from(next_u32(state)).unwrap() % vocabulary.len();
                vocabulary[index]
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn next_u32(state: &mut u64) -> u32 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (*state >> 32) as u32
    }

    fn full_vector_similarity(left: &str, right: &str) -> f64 {
        let mut ids = BTreeMap::<String, TermId>::new();
        for token in tokenize(left).chain(tokenize(right)) {
            let next = TermId(ids.len() as u32);
            ids.entry(token.to_owned()).or_insert(next);
        }
        let left_frequencies = full_frequencies(left, &ids);
        let right_frequencies = full_frequencies(right, &ids);
        let left_histogram = frequency_histogram(&left_frequencies);
        let right_histogram = frequency_histogram(&right_frequencies);
        let left_document = PreparedDocument {
            term_start: 0,
            term_len: left_frequencies.len() as u32,
            histogram_start: 0,
            histogram_len: left_histogram.len() as u16,
            length: left_frequencies.iter().map(|(_, count)| count).sum(),
            digest: [0; 32],
        };
        let right_document = PreparedDocument {
            term_start: 0,
            term_len: right_frequencies.len() as u32,
            histogram_start: 0,
            histogram_len: right_histogram.len() as u16,
            length: right_frequencies.iter().map(|(_, count)| count).sum(),
            digest: [1; 32],
        };
        bm25_cosine(
            &left_document,
            &left_frequencies,
            &left_histogram,
            &right_document,
            &right_frequencies,
            &right_histogram,
            0.6,
        )
        .1
    }

    fn full_frequencies(value: &str, ids: &BTreeMap<String, TermId>) -> Vec<(TermId, u32)> {
        let mut frequencies = BTreeMap::<TermId, u32>::new();
        for token in tokenize(value) {
            *frequencies.entry(ids[token]).or_default() += 1;
        }
        frequencies.into_iter().collect()
    }

    fn frequency_histogram(frequencies: &[(TermId, u32)]) -> Vec<(u32, u32)> {
        let mut histogram = BTreeMap::<u32, u32>::new();
        for &(_, frequency) in frequencies {
            *histogram.entry(frequency).or_default() += 1;
        }
        histogram.into_iter().collect()
    }
}
