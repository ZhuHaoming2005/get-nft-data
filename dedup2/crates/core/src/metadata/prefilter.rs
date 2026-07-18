use crate::entity::{ChainId, ContractId};
use crate::error::DedupError;
use crate::metadata::template::TemplateFingerprint;
use crate::progress::ProgressObserver;
use ahash::AHashMap;
use rayon::prelude::*;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CandidatePair {
    pub left: ContractId,
    pub right: ContractId,
}

impl CandidatePair {
    pub fn new(a: ContractId, b: ContractId) -> Option<Self> {
        (a != b).then_some(if a < b {
            Self { left: a, right: b }
        } else {
            Self { left: b, right: a }
        })
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PrefilterStats {
    pub eligible_contracts: u64,
    pub low_information_contracts: u64,
    pub exact_bucket_pairs: u64,
    pub lsh_pairs: u64,
    pub quota_truncations: u64,
    pub bucket_cap_truncations: u64,
    pub lsh_bands: u32,
    pub lsh_rows_per_band: u32,
    pub template_jaccard_threshold: f64,
    pub predicted_template_jaccard_recall: f64,
}

#[derive(Clone, Debug)]
pub struct PrefilterConfig {
    pub template_jaccard_threshold: f64,
    pub lsh_bands: u32,
    pub lsh_rows_per_band: u32,
    pub max_outgoing_candidates_per_contract: usize,
    pub max_candidates_per_target_chain: usize,
    pub neighbors_per_target_chain: usize,
    pub bucket_pair_cap: usize,
}

impl PrefilterConfig {
    pub fn resolve_lsh(&mut self) {
        if self.lsh_bands == 0 || self.lsh_rows_per_band == 0 {
            let threshold = self.template_jaccard_threshold.clamp(0.5, 0.99);
            let rows = 2_u32;
            let mut bands = 1_u32;
            while bands < 64 {
                let recall = 1.0 - (1.0 - threshold.powi(rows as i32)).powi(bands as i32);
                if recall >= 0.95 {
                    break;
                }
                bands += 1;
            }
            self.lsh_bands = bands;
            self.lsh_rows_per_band = rows;
        }
    }

    pub fn predicted_recall(&self) -> f64 {
        let threshold = self.template_jaccard_threshold.clamp(0.5, 0.99);
        let rows = self.lsh_rows_per_band.max(1) as i32;
        let bands = self.lsh_bands.max(1) as i32;
        1.0 - (1.0 - threshold.powi(rows)).powi(bands)
    }
}

#[derive(Clone, Copy)]
struct CompactRow {
    contract_id: ContractId,
    chain_id: ChainId,
    digest: [u8; 32],
}

#[derive(Clone, Copy)]
struct RawEdge {
    left: ContractId,
    right: ContractId,
    exact: bool,
    bands: u16,
}

#[derive(Clone, Copy)]
struct Evidence {
    pair: CandidatePair,
    exact: bool,
    bands: u16,
    shared: u32,
}

#[derive(Clone, Copy)]
struct BandRecord {
    band: u32,
    key: u64,
    row: usize,
}

#[derive(Clone, Copy)]
struct DirectedEvidence {
    source: ContractId,
    target: ContractId,
    target_chain: ChainId,
    evidence: Evidence,
}

pub fn generate_candidates(
    fingerprints: &[(ContractId, ChainId, TemplateFingerprint)],
    config: &PrefilterConfig,
    progress: &dyn ProgressObserver,
) -> Result<(Vec<CandidatePair>, PrefilterStats), DedupError> {
    let mut stats = PrefilterStats {
        template_jaccard_threshold: config.template_jaccard_threshold,
        lsh_bands: config.lsh_bands,
        lsh_rows_per_band: config.lsh_rows_per_band,
        predicted_template_jaccard_recall: config.predicted_recall(),
        ..PrefilterStats::default()
    };

    progress.begin_phase("prefilter_compact", Some(fingerprints.len() as u64));
    let eligible: Vec<(CompactRow, Vec<u64>)> = fingerprints
        .par_iter()
        .filter_map(|(contract_id, chain_id, fingerprint)| {
            (!fingerprint.low_information).then(|| {
                (
                    CompactRow {
                        contract_id: *contract_id,
                        chain_id: *chain_id,
                        digest: fingerprint.digest,
                    },
                    hash_features(&fingerprint.features),
                )
            })
        })
        .collect();
    progress.add_completed(fingerprints.len() as u64);
    stats.eligible_contracts = eligible.len() as u64;
    stats.low_information_contracts = fingerprints.len().saturating_sub(eligible.len()) as u64;
    if eligible.len() < 2 {
        return Ok((Vec::new(), stats));
    }
    let (rows, feature_ids): (Vec<_>, Vec<_>) = eligible.into_iter().unzip();

    let band_size = config.lsh_rows_per_band.max(1) as usize;
    let bands = config.lsh_bands.max(1) as usize;
    let num_hashes = band_size.saturating_mul(bands);
    progress.begin_phase("prefilter_minhash", Some(rows.len() as u64));
    let signatures: Vec<Vec<u32>> = feature_ids
        .par_iter()
        .map(|features| minhash_signature(features, num_hashes))
        .collect();
    progress.add_completed(rows.len() as u64);

    let (mut edges, exact_pairs, bucket_truncations) =
        exact_digest_edges(&rows, config.bucket_pair_cap, progress)?;
    stats.exact_bucket_pairs = exact_pairs;
    stats.bucket_cap_truncations = bucket_truncations;

    let lsh_edges = lsh_edges(
        &rows,
        &signatures,
        bands,
        band_size,
        config.neighbors_per_target_chain.max(1),
        progress,
    )?;
    edges.extend(lsh_edges);
    drop(signatures);

    progress.begin_phase("prefilter_aggregate", Some(edges.len() as u64));
    edges.par_sort_unstable_by(|left, right| {
        left.left
            .cmp(&right.left)
            .then(left.right.cmp(&right.right))
    });
    let mut evidence = Vec::new();
    let mut position = 0;
    while position < edges.len() {
        progress.check_cancelled()?;
        let left = edges[position].left;
        let right = edges[position].right;
        let mut exact = false;
        let mut band_hits = 0_u16;
        let mut end = position;
        while end < edges.len() && edges[end].left == left && edges[end].right == right {
            exact |= edges[end].exact;
            band_hits = band_hits.saturating_add(edges[end].bands);
            end += 1;
        }
        if !exact && band_hits > 0 {
            stats.lsh_pairs += 1;
        }
        evidence.push(Evidence {
            pair: CandidatePair { left, right },
            exact,
            bands: band_hits,
            shared: 0,
        });
        position = end;
    }
    progress.add_completed(edges.len() as u64);
    drop(edges);

    let max_contract_id = rows
        .iter()
        .map(|row| row.contract_id as usize)
        .max()
        .unwrap_or(0);
    let mut by_contract = vec![usize::MAX; max_contract_id + 1];
    for (index, row) in rows.iter().enumerate() {
        by_contract[row.contract_id as usize] = index;
    }
    progress.begin_phase("prefilter_shared", Some(evidence.len() as u64));
    evidence.par_iter_mut().for_each(|entry| {
        let left = by_contract[entry.pair.left as usize];
        let right = by_contract[entry.pair.right as usize];
        entry.shared = shared_feature_count(&feature_ids[left], &feature_ids[right]);
    });
    progress.add_completed(evidence.len() as u64);

    let total_evidence = evidence.len();
    let kept = reduce_quotas(&evidence, &rows, &by_contract, config, progress)?;
    stats.quota_truncations = total_evidence.saturating_sub(kept.len()) as u64;
    Ok((kept, stats))
}

fn exact_digest_edges(
    rows: &[CompactRow],
    pair_cap: usize,
    progress: &dyn ProgressObserver,
) -> Result<(Vec<RawEdge>, u64, u64), DedupError> {
    progress.begin_phase("prefilter_digest", Some(rows.len() as u64));
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.par_sort_unstable_by(|&left, &right| {
        rows[left]
            .digest
            .cmp(&rows[right].digest)
            .then(rows[left].contract_id.cmp(&rows[right].contract_id))
    });
    let ranges = equal_ranges(&order, |left, right| {
        rows[*left].digest == rows[*right].digest
    });
    let chunks: Vec<(Vec<RawEdge>, u64)> = ranges
        .par_iter()
        .map(|range| {
            let members = &order[range.clone()];
            if members.len() < 2 {
                return (Vec::new(), 0);
            }
            let possible = members
                .len()
                .saturating_mul(members.len().saturating_sub(1))
                / 2;
            let mut edges = Vec::with_capacity(possible.min(pair_cap));
            'outer: for left_pos in 0..members.len() {
                for right_pos in (left_pos + 1)..members.len() {
                    if edges.len() >= pair_cap {
                        break 'outer;
                    }
                    let left = rows[members[left_pos]].contract_id;
                    let right = rows[members[right_pos]].contract_id;
                    if let Some(pair) = CandidatePair::new(left, right) {
                        edges.push(RawEdge {
                            left: pair.left,
                            right: pair.right,
                            exact: true,
                            bands: 0,
                        });
                    }
                }
            }
            (edges, u64::from(possible > pair_cap))
        })
        .collect();
    progress.add_completed(rows.len() as u64);
    let mut edges = Vec::new();
    let mut truncations = 0;
    for (chunk, truncated) in chunks {
        edges.extend(chunk);
        truncations += truncated;
    }
    let pairs = edges.len() as u64;
    Ok((edges, pairs, truncations))
}

fn lsh_edges(
    rows: &[CompactRow],
    signatures: &[Vec<u32>],
    bands: usize,
    band_size: usize,
    neighbors: usize,
    progress: &dyn ProgressObserver,
) -> Result<Vec<RawEdge>, DedupError> {
    progress.begin_phase(
        "prefilter_lsh_index",
        Some((rows.len().saturating_mul(bands)) as u64),
    );
    let mut records: Vec<BandRecord> = signatures
        .par_iter()
        .enumerate()
        .flat_map_iter(|(row, signature)| {
            (0..bands).map(move |band| {
                let start = band * band_size;
                BandRecord {
                    band: band as u32,
                    key: band_hash(&signature[start..start + band_size]),
                    row,
                }
            })
        })
        .collect();
    progress.add_completed(records.len() as u64);
    records.par_sort_unstable_by(|left, right| {
        left.band
            .cmp(&right.band)
            .then(left.key.cmp(&right.key))
            .then(rows[left.row].contract_id.cmp(&rows[right.row].contract_id))
    });
    let ranges = equal_ranges(&records, |left, right| {
        left.band == right.band && left.key == right.key
    });
    progress.begin_phase("prefilter_lsh_emit", Some(ranges.len() as u64));
    let chunks: Vec<Vec<RawEdge>> = ranges
        .par_iter()
        .map(|range| {
            let members = records[range.clone()]
                .iter()
                .map(|record| record.row)
                .collect::<Vec<_>>();
            let mut edges = Vec::new();
            if members.len() >= 2 {
                emit_band_edges(&members, rows, neighbors, &mut edges);
            }
            progress.add_completed(1);
            edges
        })
        .collect();
    progress.check_cancelled()?;
    Ok(chunks.into_iter().flatten().collect())
}

fn reduce_quotas(
    evidence: &[Evidence],
    rows: &[CompactRow],
    by_contract: &[usize],
    config: &PrefilterConfig,
    progress: &dyn ProgressObserver,
) -> Result<Vec<CandidatePair>, DedupError> {
    progress.begin_phase(
        "prefilter_reduce",
        Some((evidence.len().saturating_mul(2)) as u64),
    );
    let mut directed = Vec::with_capacity(evidence.len().saturating_mul(2));
    for &item in evidence {
        let left_chain = rows[by_contract[item.pair.left as usize]].chain_id;
        let right_chain = rows[by_contract[item.pair.right as usize]].chain_id;
        directed.push(DirectedEvidence {
            source: item.pair.left,
            target: item.pair.right,
            target_chain: right_chain,
            evidence: item,
        });
        directed.push(DirectedEvidence {
            source: item.pair.right,
            target: item.pair.left,
            target_chain: left_chain,
            evidence: item,
        });
    }
    directed.par_sort_unstable_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then(right.evidence.exact.cmp(&left.evidence.exact))
            .then(right.evidence.shared.cmp(&left.evidence.shared))
            .then(right.evidence.bands.cmp(&left.evidence.bands))
            .then(left.target.cmp(&right.target))
    });
    let ranges = equal_ranges(&directed, |left, right| left.source == right.source);
    let chunks: Vec<Vec<CandidatePair>> = ranges
        .par_iter()
        .map(|range| {
            let mut per_chain: AHashMap<ChainId, usize> = AHashMap::new();
            let mut kept = Vec::new();
            for item in &directed[range.clone()] {
                if kept.len() >= config.max_outgoing_candidates_per_contract {
                    break;
                }
                let chain_count = per_chain.entry(item.target_chain).or_default();
                if *chain_count >= config.max_candidates_per_target_chain {
                    continue;
                }
                *chain_count += 1;
                kept.push(item.evidence.pair);
            }
            progress.add_completed(range.len() as u64);
            kept
        })
        .collect();
    progress.check_cancelled()?;
    let mut kept: Vec<CandidatePair> = chunks.into_iter().flatten().collect();
    kept.par_sort_unstable();
    kept.dedup();
    Ok(kept)
}

fn equal_ranges<T>(values: &[T], equal: impl Fn(&T, &T) -> bool) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < values.len() {
        let mut end = start + 1;
        while end < values.len() && equal(&values[start], &values[end]) {
            end += 1;
        }
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn emit_band_edges(
    members: &[usize],
    rows: &[CompactRow],
    neighbors: usize,
    edges: &mut Vec<RawEdge>,
) {
    let mut by_chain: AHashMap<ChainId, Vec<ContractId>> = AHashMap::new();
    for &index in members {
        by_chain
            .entry(rows[index].chain_id)
            .or_default()
            .push(rows[index].contract_id);
    }
    for contracts in by_chain.values_mut() {
        contracts.sort_unstable();
        contracts.dedup();
        for pair in contracts.windows(2) {
            push_lsh_edge(pair[0], pair[1], edges);
        }
    }
    let mut chain_ids: Vec<ChainId> = by_chain.keys().copied().collect();
    chain_ids.sort_unstable();
    for (position, &left_chain) in chain_ids.iter().enumerate() {
        for &right_chain in &chain_ids[position + 1..] {
            emit_inter_neighbors(
                &by_chain[&left_chain],
                &by_chain[&right_chain],
                neighbors,
                edges,
            );
            emit_inter_neighbors(
                &by_chain[&right_chain],
                &by_chain[&left_chain],
                neighbors,
                edges,
            );
        }
    }
}

fn emit_inter_neighbors(
    left: &[ContractId],
    right: &[ContractId],
    neighbors: usize,
    edges: &mut Vec<RawEdge>,
) {
    for &left_contract in left {
        let split = right.partition_point(|&right_contract| right_contract < left_contract);
        for &right_contract in right[split..]
            .iter()
            .chain(right[..split].iter().rev())
            .take(neighbors)
        {
            push_lsh_edge(left_contract, right_contract, edges);
        }
    }
}

fn push_lsh_edge(left: ContractId, right: ContractId, edges: &mut Vec<RawEdge>) {
    if let Some(pair) = CandidatePair::new(left, right) {
        edges.push(RawEdge {
            left: pair.left,
            right: pair.right,
            exact: false,
            bands: 1,
        });
    }
}

fn hash_features(features: &[String]) -> Vec<u64> {
    let mut ids: Vec<u64> = features
        .iter()
        .map(|feature| hash_bytes(feature.as_bytes()))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn band_hash(parts: &[u32]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for &part in parts {
        hash ^= u64::from(part);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn shared_feature_count(left: &[u64], right: &[u64]) -> u32 {
    let (mut left_pos, mut right_pos, mut shared) = (0, 0, 0_u32);
    while left_pos < left.len() && right_pos < right.len() {
        match left[left_pos].cmp(&right[right_pos]) {
            std::cmp::Ordering::Less => left_pos += 1,
            std::cmp::Ordering::Greater => right_pos += 1,
            std::cmp::Ordering::Equal => {
                shared += 1;
                left_pos += 1;
                right_pos += 1;
            }
        }
    }
    shared
}

fn minhash_signature(features: &[u64], num_hashes: usize) -> Vec<u32> {
    let mut signature = vec![u32::MAX; num_hashes];
    for &feature in features {
        let base = (feature as u32) ^ ((feature >> 32) as u32);
        for (index, minimum) in signature.iter_mut().enumerate() {
            let hash = base
                .wrapping_mul(0x85ebca77)
                .wrapping_add((index as u32).wrapping_mul(0xc2b2ae3d))
                .wrapping_mul(0x27d4eb2d)
                .wrapping_add(feature as u32);
            *minimum = (*minimum).min(hash);
        }
    }
    signature
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::NoopProgress;

    fn fingerprint(digest_byte: u8, low_information: bool) -> TemplateFingerprint {
        TemplateFingerprint {
            digest: [digest_byte; 32],
            features: vec!["v:collection=x".to_owned(), "s:name:string".to_owned()],
            low_information,
        }
    }

    fn config() -> PrefilterConfig {
        PrefilterConfig {
            template_jaccard_threshold: 0.9,
            lsh_bands: 2,
            lsh_rows_per_band: 2,
            max_outgoing_candidates_per_contract: 8,
            max_candidates_per_target_chain: 4,
            neighbors_per_target_chain: 2,
            bucket_pair_cap: 10,
        }
    }

    #[test]
    fn low_information_contract_is_excluded() {
        let fingerprints = vec![(0, 0, fingerprint(1, true)), (1, 1, fingerprint(1, false))];
        let (pairs, stats) = generate_candidates(&fingerprints, &config(), &NoopProgress).unwrap();
        assert!(pairs.is_empty());
        assert_eq!(stats.low_information_contracts, 1);
    }

    #[test]
    fn exact_bucket_cap_is_reported() {
        let fingerprints = (0..5)
            .map(|id| (id, (id % 2) as ChainId, fingerprint(1, false)))
            .collect::<Vec<_>>();
        let mut config = config();
        config.bucket_pair_cap = 2;
        let (_, stats) = generate_candidates(&fingerprints, &config, &NoopProgress).unwrap();
        assert_eq!(stats.exact_bucket_pairs, 2);
        assert_eq!(stats.bucket_cap_truncations, 1);
    }

    #[test]
    fn per_source_quota_limits_union_candidates() {
        let fingerprints = (0..6)
            .map(|id| (id, (id % 2) as ChainId, fingerprint(1, false)))
            .collect::<Vec<_>>();
        let mut config = config();
        config.max_outgoing_candidates_per_contract = 1;
        config.max_candidates_per_target_chain = 1;
        let (pairs, stats) = generate_candidates(&fingerprints, &config, &NoopProgress).unwrap();
        assert!(!pairs.is_empty());
        assert!(stats.quota_truncations > 0);
    }
}
