use crate::model::{ContractId, NameValueId};
use crate::resident::candidate_bounds::CandidateBounds;
use crate::resident::NameFeatureStore;
use ahash::{AHashMap, AHashSet};
use parking_lot::Mutex;
use rayon::prelude::*;

type NameShardBuild = AHashMap<u32, Vec<NameValueId>>;
type PreparedNameShard = (usize, Vec<u64>, Vec<NameValueId>, Vec<(u32, u32)>);

/// Per-seed Name candidate probe, computed once from global token rarity and
/// reused across every owner shard for that seed (see
/// [`crate::resident::PreparedNamePlan`]).
///
/// `prefix_tokens` holds the rarest dense occurrence-token ids of the seed
/// name, truncated to the shortest length that is still provably sufficient
/// for lossless recall at the configured Jaro-Winkler threshold (see
/// [`NameIndex::prepare_query`]). Names that cannot form a useful prefix
/// (empty text, or a threshold that admits zero-overlap matches) fall back to
/// `direct_verification`, which scans `names_by_shard` for that seed instead
/// of probing postings.
#[derive(Clone, Debug, Default)]
pub struct PreparedNameQuery {
    pub prefix_tokens: Vec<u32>,
    pub direct_verification: bool,
}

impl PreparedNameQuery {
    pub fn direct_verification() -> Self {
        Self {
            prefix_tokens: Vec::new(),
            direct_verification: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NameIndex {
    /// CSR postings, one offsets/postings pair per owner shard. `shard_offsets[shard]`
    /// has `token_count + 1` entries indexed by the dense occurrence-token id;
    /// `shard_postings[shard][offsets[t]..offsets[t + 1]]` is the sorted list of
    /// `NameValueId`s owned by that shard which contain token `t`.
    shard_offsets: Vec<Vec<u64>>,
    shard_postings: Vec<Vec<NameValueId>>,
    /// Global posting frequency per dense occurrence-token id, summed across all
    /// shards. Drives the rarity sort used by `prepare_query`.
    token_frequency: Vec<u32>,
    /// Seed name -> its own occurrence tokens, encoded as dense ids into the
    /// shared vocabulary above. Only seed names have an entry.
    seed_tokens: AHashMap<NameValueId, Vec<u32>>,
    character_offsets: Vec<u64>,
    characters: Vec<char>,
    sorted_characters: Vec<char>,
    member_offsets: Vec<u64>,
    members: Vec<ContractId>,
    names_by_shard: Vec<Vec<NameValueId>>,
}

impl NameIndex {
    pub fn build(
        features: &NameFeatureStore,
        seed_names: &[NameValueId],
        shard_count: usize,
    ) -> Self {
        Self::build_inner(features, seed_names, shard_count, None, None)
    }

    pub fn build_numa(
        features: &NameFeatureStore,
        seed_names: &[NameValueId],
        shard_count: usize,
        executor: &crate::pipeline::CpuExecutor,
    ) -> Self {
        Self::build_inner(features, seed_names, shard_count, Some(executor), None)
    }

    pub fn build_numa_with_progress(
        features: &NameFeatureStore,
        seed_names: &[NameValueId],
        shard_count: usize,
        executor: &crate::pipeline::CpuExecutor,
        progress: &crate::progress::Progress,
    ) -> Self {
        Self::build_numa_with_progress_in(
            features,
            seed_names,
            shard_count,
            executor,
            progress,
            crate::progress::PhaseSlot::Primary,
        )
    }

    pub fn build_numa_with_secondary_progress(
        features: &NameFeatureStore,
        seed_names: &[NameValueId],
        shard_count: usize,
        executor: &crate::pipeline::CpuExecutor,
        progress: &crate::progress::Progress,
    ) -> Self {
        Self::build_numa_with_progress_in(
            features,
            seed_names,
            shard_count,
            executor,
            progress,
            crate::progress::PhaseSlot::Secondary,
        )
    }

    fn build_numa_with_progress_in(
        features: &NameFeatureStore,
        seed_names: &[NameValueId],
        shard_count: usize,
        executor: &crate::pipeline::CpuExecutor,
        progress: &crate::progress::Progress,
        progress_slot: crate::progress::PhaseSlot,
    ) -> Self {
        Self::build_inner(
            features,
            seed_names,
            shard_count,
            Some(executor),
            Some((progress, progress_slot)),
        )
    }

    fn build_inner(
        features: &NameFeatureStore,
        seed_names: &[NameValueId],
        shard_count: usize,
        executor: Option<&crate::pipeline::CpuExecutor>,
        progress: Option<(&crate::progress::Progress, crate::progress::PhaseSlot)>,
    ) -> Self {
        let seed_tokens_by_name_raw = seed_names
            .iter()
            .copied()
            .map(|name| (name, occurrence_tokens(features.values.get(name.0))))
            .collect::<AHashMap<_, _>>();
        let mut vocabulary = seed_tokens_by_name_raw
            .values()
            .flat_map(|tokens| tokens.iter().copied())
            .collect::<AHashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        // Deterministic dense ids: sort the raw occurrence tokens before assigning ids.
        vocabulary.sort_unstable();
        let token_ids = vocabulary
            .iter()
            .enumerate()
            .map(|(index, &token)| (token, index as u32))
            .collect::<AHashMap<_, _>>();
        let token_count = vocabulary.len();
        let seed_tokens = seed_tokens_by_name_raw
            .into_iter()
            .map(|(name, tokens)| {
                let mut dense = tokens
                    .iter()
                    .map(|token| token_ids[token])
                    .collect::<Vec<_>>();
                dense.sort_unstable();
                (name, dense)
            })
            .collect::<AHashMap<_, _>>();

        let name_count = features.values.len();
        let mut shard_build = (0..shard_count)
            .map(|_| AHashMap::<u32, Vec<NameValueId>>::new())
            .collect::<Vec<_>>();
        let mut names_by_shard = (0..shard_count).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut character_offsets = Vec::with_capacity(name_count + 1);
        let mut characters = Vec::new();
        let mut sorted_characters = Vec::new();
        character_offsets.push(0);
        const NAME_CHUNK: usize = 4096;
        const WAVE_NAMES: usize = NAME_CHUNK * 128;
        for wave_start in (0..name_count).step_by(WAVE_NAMES) {
            let wave_end = (wave_start + WAVE_NAMES).min(name_count);
            let ranges = (wave_start..wave_end)
                .step_by(NAME_CHUNK)
                .map(|start| start..(start + NAME_CHUNK).min(wave_end))
                .collect::<Vec<_>>();
            let mut chunks = if let Some(executor) = executor {
                executor
                    .install_on_all(|lane, lane_count| {
                        ranges
                            .par_iter()
                            .enumerate()
                            .filter(|(chunk, _)| chunk % lane_count == lane)
                            .map(|(_, range)| {
                                prepare_name_chunk(features, range.clone(), &token_ids)
                            })
                            .collect::<Vec<_>>()
                    })
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
            } else {
                ranges
                    .into_par_iter()
                    .map(|range| prepare_name_chunk(features, range, &token_ids))
                    .collect::<Vec<_>>()
            };
            chunks.sort_unstable_by_key(|chunk| chunk.first_name);
            for chunk in chunks {
                let completed = chunk.names.len() as u64;
                characters.extend(chunk.characters);
                sorted_characters.extend(chunk.sorted_characters);
                for (offset, postings) in chunk.names.into_iter().enumerate() {
                    let name_id = NameValueId((chunk.first_name + offset) as u32);
                    let owner = crate::model::owner_shard(name_id.0, shard_count);
                    names_by_shard[owner].push(name_id);
                    character_offsets.push(
                        character_offsets.last().copied().unwrap()
                            + u64::from(postings.character_len),
                    );
                    for token in postings.seed_tokens {
                        shard_build[owner].entry(token).or_default().push(name_id);
                    }
                }
                if let Some((progress, slot)) = progress {
                    progress.add_phase_completed_in(slot, completed);
                }
            }
        }

        let (shard_offsets, shard_postings, token_frequency) = if let Some(executor) = executor {
            build_csr_numa(shard_build, token_count, executor)
        } else {
            build_csr(shard_build, token_count)
        };

        let mut member_counts = vec![0_u64; name_count];
        for name in features.contract_names.iter().flatten() {
            member_counts[name.index()] += 1;
        }
        let mut member_offsets = Vec::with_capacity(name_count + 1);
        member_offsets.push(0);
        for count in member_counts {
            member_offsets.push(member_offsets.last().copied().unwrap() + count);
        }
        let mut member_cursor = member_offsets[..name_count].to_vec();
        let mut members = vec![ContractId(0); features.contract_names.iter().flatten().count()];
        for (contract, &name) in features.contract_names.iter().enumerate() {
            if let Some(name) = name {
                let slot = member_cursor[name.index()] as usize;
                members[slot] = ContractId(contract as u32);
                member_cursor[name.index()] += 1;
            }
        }
        Self {
            shard_offsets,
            shard_postings,
            token_frequency,
            seed_tokens,
            character_offsets,
            characters,
            sorted_characters,
            member_offsets,
            members,
            names_by_shard,
        }
    }

    /// Computes the safe rarity-sorted token prefix for `seed` once. The
    /// result can be probed against every owner shard via `candidates_into`
    /// without recomputation, and is what `PreparedNamePlan` stores.
    ///
    /// Correctness: let `L` be the number of occurrence tokens (characters) in
    /// the seed name. For any candidate name of length `right_len`, reaching
    /// `threshold` requires a multiset character overlap of at least
    /// `CandidateBounds::minimum_multiset_overlap(L, right_len, threshold_pct)`
    /// with the seed (a proven, lossless bound already used for post-filtering
    /// in `dedup::name`). We compute `required = ` the *minimum* of that bound
    /// over every `right_len` that could possibly reach the threshold at all
    /// (`CandidateBounds::lengths_can_reach`); this is the least overlap any
    /// reachable candidate could ever get away with. By a pigeonhole argument,
    /// probing any `L - required + 1` of the seed's tokens is guaranteed to hit
    /// at least one token shared with a candidate that meets that overlap, so
    /// picking the *rarest* `L - required + 1` tokens is both safe and the
    /// tightest such prefix we can size independent of the candidate's actual
    /// length.
    pub fn prepare_query(&self, name: NameValueId, threshold: f64) -> PreparedNameQuery {
        let Some(tokens) = self.seed_tokens.get(&name) else {
            return PreparedNameQuery::direct_verification();
        };
        let total = tokens.len();
        if total == 0 || threshold <= 0.0 {
            return PreparedNameQuery::direct_verification();
        }
        let threshold_pct = (threshold * 100.0).min(100.0);
        let required_overlap = minimum_required_overlap(total, threshold_pct);
        if required_overlap == 0 {
            // Some reachable candidate length needs zero character overlap;
            // no token-based prefix can safely exclude anything.
            return PreparedNameQuery::direct_verification();
        }
        let prefix_len = total
            .saturating_sub(required_overlap)
            .saturating_add(1)
            .min(total);
        let mut prefix = tokens.clone();
        prefix.sort_by_key(|&token_id| self.token_frequency[token_id as usize]);
        prefix.truncate(prefix_len);
        PreparedNameQuery {
            prefix_tokens: prefix,
            direct_verification: false,
        }
    }

    pub fn candidates(&self, shard: usize, query: &PreparedNameQuery) -> Vec<NameValueId> {
        let mut candidates = Vec::new();
        self.candidates_into(shard, query, &mut candidates);
        candidates
    }

    pub fn candidates_into(
        &self,
        shard: usize,
        query: &PreparedNameQuery,
        candidates: &mut Vec<NameValueId>,
    ) {
        candidates.clear();
        if query.direct_verification {
            candidates.extend_from_slice(&self.names_by_shard[shard]);
            return;
        }
        for &token_id in &query.prefix_tokens {
            candidates.extend_from_slice(self.shard_posting(shard, token_id));
        }
        candidates.sort_unstable();
        candidates.dedup();
    }

    fn shard_posting(&self, shard: usize, token_id: u32) -> &[NameValueId] {
        let offsets = &self.shard_offsets[shard];
        let start = offsets[token_id as usize] as usize;
        let end = offsets[token_id as usize + 1] as usize;
        &self.shard_postings[shard][start..end]
    }

    pub fn characters(&self, name: NameValueId) -> &[char] {
        let start = self.character_offsets[name.index()] as usize;
        let end = self.character_offsets[name.index() + 1] as usize;
        &self.characters[start..end]
    }

    pub fn sorted_characters(&self, name: NameValueId) -> &[char] {
        let start = self.character_offsets[name.index()] as usize;
        let end = self.character_offsets[name.index() + 1] as usize;
        &self.sorted_characters[start..end]
    }

    pub fn members(&self, name: NameValueId) -> &[ContractId] {
        let start = self.member_offsets[name.index()] as usize;
        let end = self.member_offsets[name.index() + 1] as usize;
        &self.members[start..end]
    }

    pub fn posting_count(&self) -> u64 {
        self.shard_postings
            .iter()
            .map(|postings| postings.len() as u64)
            .sum::<u64>()
            .saturating_add(self.members.len() as u64)
    }
}

/// Converts per-shard build maps into CSR (offsets + contiguous postings) and
/// returns the global per-token posting frequency (summed across shards).
fn build_csr(
    shard_build: Vec<NameShardBuild>,
    token_count: usize,
) -> (Vec<Vec<u64>>, Vec<Vec<NameValueId>>, Vec<u32>) {
    assemble_csr(
        shard_build
            .into_iter()
            .enumerate()
            .map(|(shard, shard_map)| build_shard_csr(shard, shard_map, token_count))
            .collect(),
        token_count,
    )
}

/// Builds each owner shard's contiguous posting storage on its assigned NUMA
/// lane. The returned vectors retain that first-touch placement when they are
/// moved into the global index.
fn build_csr_numa(
    shard_build: Vec<NameShardBuild>,
    token_count: usize,
    executor: &crate::pipeline::CpuExecutor,
) -> (Vec<Vec<u64>>, Vec<Vec<NameValueId>>, Vec<u32>) {
    let lane_count = executor.numa_pool_count();
    let mut inputs_by_lane = (0..lane_count)
        .map(|_| Vec::<(usize, NameShardBuild)>::new())
        .collect::<Vec<_>>();
    for (shard, shard_map) in shard_build.into_iter().enumerate() {
        inputs_by_lane[shard % lane_count].push((shard, shard_map));
    }
    let inputs_by_lane = inputs_by_lane
        .into_iter()
        .map(|inputs| Mutex::new(Some(inputs)))
        .collect::<Vec<_>>();
    let prepared = executor
        .install_on_all(|lane, _| {
            inputs_by_lane[lane]
                .lock()
                .take()
                .expect("each NUMA lane consumes its Name shard inputs once")
                .into_par_iter()
                .map(|(shard, shard_map)| build_shard_csr(shard, shard_map, token_count))
                .collect::<Vec<_>>()
        })
        .into_iter()
        .flatten()
        .collect();
    assemble_csr(prepared, token_count)
}

fn build_shard_csr(
    shard: usize,
    mut shard_map: NameShardBuild,
    token_count: usize,
) -> PreparedNameShard {
    for postings in shard_map.values_mut() {
        postings.sort_unstable();
    }
    let frequencies = shard_map
        .iter()
        .map(|(&token, postings)| (token, postings.len() as u32))
        .collect::<Vec<_>>();
    let mut offsets = vec![0_u64; token_count + 1];
    for (&token, postings) in &shard_map {
        offsets[token as usize + 1] = postings.len() as u64;
    }
    for index in 0..token_count {
        offsets[index + 1] += offsets[index];
    }
    let mut postings_flat = vec![NameValueId(0); offsets[token_count] as usize];
    for (token, postings) in shard_map {
        let start = offsets[token as usize] as usize;
        postings_flat[start..start + postings.len()].copy_from_slice(&postings);
    }
    (shard, offsets, postings_flat, frequencies)
}

fn assemble_csr(
    mut prepared: Vec<PreparedNameShard>,
    token_count: usize,
) -> (Vec<Vec<u64>>, Vec<Vec<NameValueId>>, Vec<u32>) {
    prepared.sort_unstable_by_key(|(shard, _, _, _)| *shard);
    let mut token_frequency = vec![0_u32; token_count];
    let mut shard_offsets = Vec::with_capacity(prepared.len());
    let mut shard_postings = Vec::with_capacity(prepared.len());
    for (_, offsets, postings, frequencies) in prepared {
        for (token, frequency) in frequencies {
            token_frequency[token as usize] += frequency;
        }
        shard_offsets.push(offsets);
        shard_postings.push(postings);
    }
    (shard_offsets, shard_postings, token_frequency)
}

/// Above this many characters past `left_len`, `upper_bound_from_lengths`
/// has effectively converged to its asymptote (a fixed function of
/// `left_len.min(4)` as `right_len -> infinity`; see the Jaro-Winkler prefix
/// bonus term). We only need to scan far enough to observe that asymptote.
const MAX_OVERLAP_SCAN_MARGIN: usize = 4096;

/// The least multiset-overlap that *any* candidate length reachable at
/// `threshold_pct` could get away with against a seed of `left_len`
/// occurrence tokens. Scanning outward from `left_len` in both directions
/// until `CandidateBounds::lengths_can_reach` turns false is safe because
/// unreachable lengths cannot contribute a valid (missed) candidate.
///
/// The downward scan (`right_len` shrinking to 0) always terminates. The
/// upward scan is capped defensively: `lengths_can_reach` only ever
/// approaches a fixed asymptotic score as `right_len -> infinity` (it never
/// reaches exactly zero), so for any sane threshold the scan converges well
/// before the cap. If the cap is hit while still reachable (only possible
/// for unrealistically low thresholds), we conservatively report `0`, which
/// forces the caller to fall back to full direct verification instead of
/// guessing an unsafe prefix length.
fn minimum_required_overlap(left_len: usize, threshold_pct: f64) -> usize {
    if left_len == 0 || threshold_pct <= 0.0 {
        return 0;
    }
    let mut minimum = left_len;
    let mut right_len = left_len;
    loop {
        if !CandidateBounds::lengths_can_reach(left_len, right_len, threshold_pct) {
            break;
        }
        minimum = minimum.min(CandidateBounds::minimum_multiset_overlap(
            left_len,
            right_len,
            threshold_pct,
        ));
        if right_len == 0 {
            break;
        }
        right_len -= 1;
    }
    let scan_limit = left_len.saturating_add(MAX_OVERLAP_SCAN_MARGIN);
    let mut right_len = left_len + 1;
    loop {
        if right_len > scan_limit {
            // Could not prove the reachable window is bounded within a sane
            // margin; refuse to guess and let the caller fall back safely.
            return 0;
        }
        if !CandidateBounds::lengths_can_reach(left_len, right_len, threshold_pct) {
            break;
        }
        minimum = minimum.min(CandidateBounds::minimum_multiset_overlap(
            left_len,
            right_len,
            threshold_pct,
        ));
        right_len += 1;
    }
    minimum
}

struct PreparedNameChunk {
    first_name: usize,
    characters: Vec<char>,
    sorted_characters: Vec<char>,
    names: Vec<PreparedName>,
}

struct PreparedName {
    seed_tokens: Vec<u32>,
    character_len: u32,
}

fn prepare_name_chunk(
    features: &NameFeatureStore,
    range: std::ops::Range<usize>,
    token_ids: &AHashMap<u64, u32>,
) -> PreparedNameChunk {
    let first_name = range.start;
    let mut characters = Vec::new();
    let mut sorted_characters = Vec::new();
    let mut names = Vec::with_capacity(range.len());
    let mut counts = AHashMap::new();
    let mut tokens = Vec::new();
    for name_index in range {
        let text = features.values.get(name_index as u32);
        characters.extend(text.chars());
        let sorted_start = sorted_characters.len();
        sorted_characters.extend(text.chars());
        sorted_characters[sorted_start..].sort_unstable();
        occurrence_tokens_into(text, &mut counts, &mut tokens);
        names.push(PreparedName {
            seed_tokens: tokens
                .iter()
                .filter_map(|token| token_ids.get(token).copied())
                .collect(),
            character_len: text.chars().count() as u32,
        });
    }
    PreparedNameChunk {
        first_name,
        characters,
        sorted_characters,
        names,
    }
}

pub fn occurrence_tokens(value: &str) -> Vec<u64> {
    let mut counts = AHashMap::new();
    let mut output = Vec::new();
    occurrence_tokens_into(value, &mut counts, &mut output);
    output
}

fn occurrence_tokens_into(value: &str, counts: &mut AHashMap<char, u32>, output: &mut Vec<u64>) {
    counts.clear();
    output.clear();
    output.extend(value.chars().map(|character| {
        let occurrence = counts.entry(character).or_default();
        let token = (u64::from(character as u32) << 32) | u64::from(*occurrence);
        *occurrence += 1;
        token
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident::value_pools::ByteInterner;

    #[test]
    fn repeated_characters_have_distinct_occurrence_tokens() {
        let tokens = occurrence_tokens("aab");
        assert_eq!(tokens.len(), 3);
        assert_ne!(tokens[0], tokens[1]);
    }

    fn feature_store(names: &[&str]) -> NameFeatureStore {
        let mut interner = ByteInterner::default();
        let contract_names = names
            .iter()
            .map(|name| Some(NameValueId(interner.intern(name))))
            .collect();
        NameFeatureStore {
            values: interner.freeze(),
            contract_names,
        }
    }

    #[test]
    fn safe_prefix_is_shorter_than_full_token_set_for_long_names() {
        let features = feature_store(&["abcdefghijklmnopqrstuvwxyz"]);
        let seed = features.contract_names[0].unwrap();
        let index = NameIndex::build(&features, &[seed], 4);
        let query = index.prepare_query(seed, 0.98);
        assert!(!query.direct_verification);
        assert!(query.prefix_tokens.len() < 26);
        assert!(!query.prefix_tokens.is_empty());
    }

    #[test]
    fn safe_prefix_falls_back_to_direct_verification_for_empty_name() {
        let features = feature_store(&[""]);
        let seed = features.contract_names[0].unwrap();
        let index = NameIndex::build(&features, &[seed], 4);
        let query = index.prepare_query(seed, 0.98);
        assert!(query.direct_verification);
    }

    #[test]
    fn safe_prefix_falls_back_to_direct_verification_for_zero_threshold() {
        let features = feature_store(&["collection"]);
        let seed = features.contract_names[0].unwrap();
        let index = NameIndex::build(&features, &[seed], 4);
        let query = index.prepare_query(seed, 0.0);
        assert!(query.direct_verification);
    }

    #[test]
    fn csr_candidates_match_exhaustive_jaro_winkler_at_every_threshold() {
        let names = [
            "alpha collection",
            "alpha collections",
            "alpha collectiom",
            "beta forest",
            "beta forrest",
            "unrelated gamma",
            "",
            "a",
        ];
        let features = feature_store(&names);
        let seed_names = features
            .contract_names
            .iter()
            .copied()
            .flatten()
            .collect::<Vec<_>>();
        let shard_count = 8;
        let index = NameIndex::build(&features, &seed_names, shard_count);
        for &threshold_pct in &[0.0, 50.0, 80.0, 95.0, 98.0, 100.0] {
            for &seed in &seed_names {
                let query = index.prepare_query(seed, threshold_pct / 100.0);
                let mut found = AHashSet::new();
                for shard in 0..shard_count {
                    found.extend(index.candidates(shard, &query));
                }
                let expected = seed_names
                    .iter()
                    .copied()
                    .filter(|&candidate| {
                        let left = index.characters(seed);
                        let right = index.characters(candidate);
                        let score = rapidfuzz::distance::jaro_winkler::similarity(
                            left.iter().copied(),
                            right.iter().copied(),
                        ) * 100.0;
                        score >= threshold_pct
                    })
                    .collect::<AHashSet<_>>();
                assert!(
                    expected.is_subset(&found),
                    "missed a hit for seed={seed:?} threshold={threshold_pct}: expected {expected:?}, found {found:?}"
                );
            }
        }
    }
}
