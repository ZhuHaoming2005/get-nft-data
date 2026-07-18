use crate::entity::{ChainId, ContractId};
use crate::error::DedupError;
use crate::metadata::template::TemplateFingerprint;
use crate::progress::ProgressObserver;
use crate::radix::{sort_by_digits, u32_digit, u64_digit};
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
    pub lsh_band_records: u64,
    pub raw_candidate_edges: u64,
    pub aggregated_candidate_pairs: u64,
    pub retained_candidate_pairs: u64,
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
    key: u64,
    band: u32,
    row: u32,
}

#[derive(Clone, Copy)]
struct DirectedEvidence {
    target: ContractId,
    target_chain: ChainId,
    evidence_index: u32,
}

struct FeatureCsr {
    offsets: Vec<usize>,
    values: Vec<u64>,
}

impl FeatureCsr {
    fn from_rows(
        eligible: Vec<(CompactRow, Vec<u64>)>,
    ) -> Result<(Vec<CompactRow>, Self), DedupError> {
        let total_features = eligible.iter().try_fold(0usize, |total, (_, features)| {
            total.checked_add(features.len()).ok_or_else(|| {
                DedupError::invalid(
                    "metadata",
                    "feature CSR size exceeds the resident index limit",
                )
            })
        })?;
        let mut rows = Vec::with_capacity(eligible.len());
        let mut offsets = Vec::with_capacity(eligible.len() + 1);
        let mut values = Vec::with_capacity(total_features);
        offsets.push(0);
        for (row, features) in eligible {
            rows.push(row);
            values.extend(features);
            offsets.push(values.len());
        }
        Ok((rows, Self { offsets, values }))
    }

    fn row(&self, index: usize) -> &[u64] {
        &self.values[self.offsets[index]..self.offsets[index + 1]]
    }
}

pub fn generate_candidates(
    fingerprints: Vec<(ContractId, ChainId, TemplateFingerprint)>,
    config: &PrefilterConfig,
    progress: &dyn ProgressObserver,
) -> Result<(Vec<CandidatePair>, PrefilterStats), DedupError> {
    let fingerprint_count = fingerprints.len();
    let mut stats = PrefilterStats {
        template_jaccard_threshold: config.template_jaccard_threshold,
        lsh_bands: config.lsh_bands,
        lsh_rows_per_band: config.lsh_rows_per_band,
        predicted_template_jaccard_recall: config.predicted_recall(),
        ..PrefilterStats::default()
    };

    progress.begin_phase("prefilter_compact", Some(fingerprint_count as u64));
    let eligible: Vec<(CompactRow, Vec<u64>)> = fingerprints
        .into_par_iter()
        .filter_map(|(contract_id, chain_id, fingerprint)| {
            if fingerprint.low_information {
                None
            } else {
                Some((
                    CompactRow {
                        contract_id,
                        chain_id,
                        digest: fingerprint.digest,
                    },
                    fingerprint.feature_ids,
                ))
            }
        })
        .collect();
    debug_assert!(
        eligible
            .windows(2)
            .all(|window| window[0].0.contract_id < window[1].0.contract_id),
        "metadata fingerprints must preserve contract-ID order"
    );
    progress.add_completed(fingerprint_count as u64);
    stats.eligible_contracts = eligible.len() as u64;
    stats.low_information_contracts = fingerprint_count.saturating_sub(eligible.len()) as u64;
    if eligible.len() < 2 {
        return Ok((Vec::new(), stats));
    }
    if eligible.len() >= u32::MAX as usize {
        return Err(DedupError::invalid(
            "metadata",
            "eligible contract count exceeds the compact u32 row-index limit",
        ));
    }
    let (rows, feature_ids) = FeatureCsr::from_rows(eligible)?;

    let band_size = config.lsh_rows_per_band.max(1) as usize;
    let bands = config.lsh_bands.max(1) as usize;
    let num_hashes = band_size.saturating_mul(bands);
    stats.lsh_band_records = (rows.len() as u64).saturating_mul(bands as u64);
    progress.begin_phase("prefilter_minhash", Some(rows.len() as u64));
    let signature_len = rows
        .len()
        .checked_mul(num_hashes)
        .ok_or_else(|| DedupError::invalid("metadata", "MinHash signature size overflow"))?;
    let mut signatures = vec![u32::MAX; signature_len];
    signatures
        .par_chunks_mut(num_hashes)
        .enumerate()
        .for_each(|(row, signature)| {
            fill_minhash_signature(feature_ids.row(row), signature);
        });
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
    stats.raw_candidate_edges = edges.len() as u64;

    progress.begin_phase("prefilter_aggregate", Some(edges.len() as u64));
    sort_by_digits(
        &mut edges,
        |pass, edge| {
            if pass < 3 {
                u32_digit(edge.right, pass)
            } else {
                u32_digit(edge.left, pass - 3)
            }
        },
        6,
    );
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
    stats.aggregated_candidate_pairs = evidence.len() as u64;

    let max_contract_id = rows
        .iter()
        .map(|row| row.contract_id as usize)
        .max()
        .unwrap_or(0);
    let mut by_contract = vec![u32::MAX; max_contract_id + 1];
    for (index, row) in rows.iter().enumerate() {
        by_contract[row.contract_id as usize] = index as u32;
    }
    progress.begin_phase("prefilter_shared", Some(evidence.len() as u64));
    evidence.par_iter_mut().for_each(|entry| {
        let left = by_contract[entry.pair.left as usize] as usize;
        let right = by_contract[entry.pair.right as usize] as usize;
        entry.shared = shared_feature_count(feature_ids.row(left), feature_ids.row(right));
    });
    progress.add_completed(evidence.len() as u64);

    let total_evidence = evidence.len();
    let kept = reduce_quotas(&evidence, &rows, &by_contract, config, progress)?;
    stats.quota_truncations = total_evidence.saturating_sub(kept.len()) as u64;
    stats.retained_candidate_pairs = kept.len() as u64;
    Ok((kept, stats))
}

fn exact_digest_edges(
    rows: &[CompactRow],
    pair_cap: usize,
    progress: &dyn ProgressObserver,
) -> Result<(Vec<RawEdge>, u64, u64), DedupError> {
    progress.begin_phase("prefilter_digest", Some(rows.len() as u64));
    let mut order: Vec<u32> = (0..rows.len() as u32).collect();
    order.par_sort_unstable_by(|&left, &right| {
        rows[left as usize]
            .digest
            .cmp(&rows[right as usize].digest)
            .then(
                rows[left as usize]
                    .contract_id
                    .cmp(&rows[right as usize].contract_id),
            )
    });
    let ranges = duplicate_ranges(&order, |left, right| {
        rows[*left as usize].digest == rows[*right as usize].digest
    });
    let chunks: Vec<(Vec<RawEdge>, u64)> = ranges
        .par_chunks(coarse_block_len(ranges.len()))
        .map(|block| {
            let capacity = block.iter().fold(0usize, |total, range| {
                let members = range.len();
                let possible = members.saturating_mul(members.saturating_sub(1)) / 2;
                total.saturating_add(possible.min(pair_cap))
            });
            let mut edges = Vec::with_capacity(capacity);
            let mut truncations = 0;
            for range in block {
                let members = &order[range.clone()];
                let possible = members
                    .len()
                    .saturating_mul(members.len().saturating_sub(1))
                    / 2;
                truncations += u64::from(possible > pair_cap);
                let edge_limit = edges.len().saturating_add(pair_cap);
                'outer: for left_pos in 0..members.len() {
                    for right_pos in (left_pos + 1)..members.len() {
                        if edges.len() >= edge_limit {
                            break 'outer;
                        }
                        let left = rows[members[left_pos] as usize].contract_id;
                        let right = rows[members[right_pos] as usize].contract_id;
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
            }
            (edges, truncations)
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
    signatures: &[u32],
    bands: usize,
    band_size: usize,
    neighbors: usize,
    progress: &dyn ProgressObserver,
) -> Result<Vec<RawEdge>, DedupError> {
    let record_count = rows
        .len()
        .checked_mul(bands)
        .ok_or_else(|| DedupError::invalid("metadata", "LSH band record count overflow"))?;
    progress.begin_phase("prefilter_lsh_index", Some(record_count as u64));
    let signature_width = bands.saturating_mul(band_size);
    let mut records = vec![
        BandRecord {
            key: 0,
            band: 0,
            row: 0,
        };
        record_count
    ];
    records
        .par_chunks_mut(bands)
        .enumerate()
        .for_each(|(row, records)| {
            let signature = &signatures[row * signature_width..(row + 1) * signature_width];
            for (band, record) in records.iter_mut().enumerate() {
                let start = band * band_size;
                *record = BandRecord {
                    key: band_hash(&signature[start..start + band_size]),
                    band: band as u32,
                    row: row as u32,
                };
            }
        });
    progress.add_completed(records.len() as u64);
    sort_band_records(&mut records);
    let ranges = duplicate_ranges(&records, |left, right| {
        left.band == right.band && left.key == right.key
    });
    progress.begin_phase("prefilter_lsh_emit", Some(ranges.len() as u64));
    let chunks: Vec<Vec<RawEdge>> = ranges
        .par_chunks(coarse_block_len(ranges.len()))
        .map(|block| {
            let mut edges = Vec::new();
            for range in block {
                emit_band_edges(&records[range.clone()], rows, neighbors, &mut edges);
            }
            compact_lsh_edges(&mut edges);
            progress.add_completed(block.len() as u64);
            edges
        })
        .collect();
    progress.check_cancelled()?;
    Ok(chunks.into_iter().flatten().collect())
}

fn sort_band_records(records: &mut Vec<BandRecord>) {
    // Records are filled row-major, with rows ordered by contract ID. The
    // stable radix passes preserve that order inside an equal (band, key)
    // bucket, so a separate contract-ID key is unnecessary.
    sort_by_digits(
        records,
        |pass, record| match pass {
            0..=5 => u64_digit(record.key, pass),
            _ => u32_digit(record.band, pass - 6),
        },
        9,
    );
}

fn reduce_quotas(
    evidence: &[Evidence],
    rows: &[CompactRow],
    by_contract: &[u32],
    config: &PrefilterConfig,
    progress: &dyn ProgressObserver,
) -> Result<Vec<CandidatePair>, DedupError> {
    progress.begin_phase(
        "prefilter_reduce",
        Some((evidence.len().saturating_mul(2)) as u64),
    );
    let source_count = by_contract.len();
    let mut directed_offsets = vec![0usize; source_count + 1];
    for item in evidence {
        directed_offsets[item.pair.left as usize + 1] += 1;
        directed_offsets[item.pair.right as usize + 1] += 1;
    }
    for source in 0..source_count {
        directed_offsets[source + 1] += directed_offsets[source];
    }
    let placeholder = DirectedEvidence {
        target: 0,
        target_chain: 0,
        evidence_index: 0,
    };
    let mut directed = vec![placeholder; evidence.len().saturating_mul(2)];
    let mut next = directed_offsets[..source_count].to_vec();
    for (evidence_index, item) in evidence.iter().enumerate() {
        let evidence_index = u32::try_from(evidence_index).map_err(|_| {
            DedupError::invalid(
                "metadata",
                "candidate evidence count exceeds the u32 resident-index limit",
            )
        })?;
        let left_chain = rows[by_contract[item.pair.left as usize] as usize].chain_id;
        let right_chain = rows[by_contract[item.pair.right as usize] as usize].chain_id;
        let left_slot = next[item.pair.left as usize];
        directed[left_slot] = DirectedEvidence {
            target: item.pair.right,
            target_chain: right_chain,
            evidence_index,
        };
        next[item.pair.left as usize] += 1;
        let right_slot = next[item.pair.right as usize];
        directed[right_slot] = DirectedEvidence {
            target: item.pair.left,
            target_chain: left_chain,
            evidence_index,
        };
        next[item.pair.right as usize] += 1;
    }

    let block_count = rayon::current_num_threads()
        .saturating_mul(4)
        .min(source_count)
        .max(1);
    let sources_per_block = source_count.div_ceil(block_count);
    let mut source_blocks = Vec::with_capacity(block_count);
    let mut rest = directed.as_mut_slice();
    let mut edge_base = 0usize;
    for source_start in (0..source_count).step_by(sources_per_block) {
        let source_end = (source_start + sources_per_block).min(source_count);
        let edge_end = directed_offsets[source_end];
        let (items, next_rest) = rest.split_at_mut(edge_end - edge_base);
        source_blocks.push((source_start, source_end, edge_base, items));
        rest = next_rest;
        edge_base = edge_end;
    }
    let chunks: Vec<Vec<CandidatePair>> = source_blocks
        .into_par_iter()
        .map(|(source_start, source_end, edge_base, block)| {
            let compare = |left: &DirectedEvidence, right: &DirectedEvidence| {
                let left_evidence = evidence[left.evidence_index as usize];
                let right_evidence = evidence[right.evidence_index as usize];
                right_evidence
                    .exact
                    .cmp(&left_evidence.exact)
                    .then(right_evidence.shared.cmp(&left_evidence.shared))
                    .then(right_evidence.bands.cmp(&left_evidence.bands))
                    .then(left.target.cmp(&right.target))
            };
            let mut kept = Vec::new();
            for source in source_start..source_end {
                let local_start = directed_offsets[source] - edge_base;
                let local_end = directed_offsets[source + 1] - edge_base;
                let items = &mut block[local_start..local_end];
                if items.is_empty() {
                    continue;
                }
                if items.len() >= 64 * 1024 {
                    items.par_sort_unstable_by(compare);
                } else {
                    items.sort_unstable_by(compare);
                }
                let mut per_chain: Vec<(ChainId, usize)> = Vec::new();
                let kept_start = kept.len();
                for item in &*items {
                    if kept.len() - kept_start >= config.max_outgoing_candidates_per_contract {
                        break;
                    }
                    let chain_position = per_chain
                        .iter()
                        .position(|&(chain, _)| chain == item.target_chain);
                    let chain_count = if let Some(position) = chain_position {
                        &mut per_chain[position].1
                    } else {
                        per_chain.push((item.target_chain, 0));
                        &mut per_chain.last_mut().expect("chain was inserted").1
                    };
                    if *chain_count >= config.max_candidates_per_target_chain {
                        continue;
                    }
                    *chain_count += 1;
                    kept.push(evidence[item.evidence_index as usize].pair);
                }
            }
            progress.add_completed(block.len() as u64);
            kept
        })
        .collect();
    progress.check_cancelled()?;
    let mut kept: Vec<CandidatePair> = chunks.into_iter().flatten().collect();
    sort_by_digits(
        &mut kept,
        |pass, pair| {
            if pass < 3 {
                u32_digit(pair.right, pass)
            } else {
                u32_digit(pair.left, pass - 3)
            }
        },
        6,
    );
    kept.dedup();
    Ok(kept)
}

fn duplicate_ranges<T>(
    values: &[T],
    equal: impl Fn(&T, &T) -> bool,
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < values.len() {
        let mut end = start + 1;
        while end < values.len() && equal(&values[start], &values[end]) {
            end += 1;
        }
        if end - start >= 2 {
            ranges.push(start..end);
        }
        start = end;
    }
    ranges
}

fn coarse_block_len(item_count: usize) -> usize {
    let blocks = rayon::current_num_threads().saturating_mul(4).max(1);
    item_count.div_ceil(blocks).max(1)
}

fn emit_band_edges(
    members: &[BandRecord],
    rows: &[CompactRow],
    neighbors: usize,
    edges: &mut Vec<RawEdge>,
) {
    let mut by_chain: AHashMap<ChainId, Vec<ContractId>> = AHashMap::new();
    for member in members {
        let row = rows[member.row as usize];
        by_chain
            .entry(row.chain_id)
            .or_default()
            .push(row.contract_id);
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

fn compact_lsh_edges(edges: &mut Vec<RawEdge>) {
    if edges.len() < 2 {
        return;
    }
    edges.sort_unstable_by_key(|edge| (edge.left, edge.right));
    let mut write = 0;
    for read in 1..edges.len() {
        if edges[write].left == edges[read].left && edges[write].right == edges[read].right {
            edges[write].bands = edges[write].bands.saturating_add(edges[read].bands);
        } else {
            write += 1;
            edges[write] = edges[read];
        }
    }
    edges.truncate(write + 1);
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

fn fill_minhash_signature(features: &[u64], signature: &mut [u32]) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::NoopProgress;

    fn fingerprint(digest_byte: u8, low_information: bool) -> TemplateFingerprint {
        TemplateFingerprint {
            digest: [digest_byte; 32],
            feature_ids: vec![1, 2],
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
    fn compact_metadata_indices_keep_expected_layout() {
        assert_eq!(std::mem::size_of::<BandRecord>(), 16);
        assert_eq!(std::mem::size_of::<u32>(), 4);
    }

    #[test]
    fn stable_band_sort_preserves_row_order_inside_bucket() {
        let mut records = vec![
            BandRecord {
                key: 9,
                band: 1,
                row: 0,
            },
            BandRecord {
                key: 4,
                band: 0,
                row: 0,
            },
            BandRecord {
                key: 9,
                band: 1,
                row: 1,
            },
            BandRecord {
                key: 4,
                band: 0,
                row: 1,
            },
            BandRecord {
                key: 9,
                band: 1,
                row: 2,
            },
            BandRecord {
                key: 4,
                band: 0,
                row: 2,
            },
        ];
        sort_band_records(&mut records);
        assert_eq!(
            records.iter().map(|record| record.row).collect::<Vec<_>>(),
            vec![0, 1, 2, 0, 1, 2]
        );
    }

    #[test]
    fn nine_pass_band_sort_matches_full_comparison_key() {
        let mut state = 0x9e3779b97f4a7c15_u64;
        let mut records = Vec::new();
        for row in 0..2_000_u32 {
            for band in 0..4_u32 {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                records.push(BandRecord {
                    key: state % 97,
                    band,
                    row,
                });
            }
        }
        let mut expected = records
            .iter()
            .map(|record| (record.band, record.key, record.row))
            .collect::<Vec<_>>();
        expected.sort_unstable();
        sort_band_records(&mut records);
        let actual = records
            .iter()
            .map(|record| (record.band, record.key, record.row))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn duplicate_ranges_discard_singletons() {
        let values = [1, 2, 2, 3, 4, 4, 4, 5];
        assert_eq!(
            duplicate_ranges(&values, |left, right| left == right),
            vec![1..3, 4..7]
        );
    }

    #[test]
    fn feature_csr_preserves_sorted_feature_rows() {
        let eligible = vec![
            (
                CompactRow {
                    contract_id: 0,
                    chain_id: 0,
                    digest: [0; 32],
                },
                vec![1, 3, 8],
            ),
            (
                CompactRow {
                    contract_id: 1,
                    chain_id: 1,
                    digest: [1; 32],
                },
                vec![2, 5],
            ),
        ];
        let (rows, features) = FeatureCsr::from_rows(eligible).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(features.row(0), &[1, 3, 8]);
        assert_eq!(features.row(1), &[2, 5]);
    }

    #[test]
    fn low_information_contract_is_excluded() {
        let fingerprints = vec![(0, 0, fingerprint(1, true)), (1, 1, fingerprint(1, false))];
        let (pairs, stats) = generate_candidates(fingerprints, &config(), &NoopProgress).unwrap();
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
        let (_, stats) = generate_candidates(fingerprints, &config, &NoopProgress).unwrap();
        assert_eq!(stats.exact_bucket_pairs, 2);
        assert_eq!(stats.bucket_cap_truncations, 1);
    }

    #[test]
    fn exact_bucket_cap_resets_for_each_range_in_a_coarse_block() {
        let fingerprints = (0..6)
            .map(|id| {
                let digest = if id < 3 { 1 } else { 2 };
                (id, (id % 2) as ChainId, fingerprint(digest, false))
            })
            .collect::<Vec<_>>();
        let mut config = config();
        config.bucket_pair_cap = 1;
        let (_, stats) = generate_candidates(fingerprints, &config, &NoopProgress).unwrap();
        assert_eq!(stats.exact_bucket_pairs, 2);
        assert_eq!(stats.bucket_cap_truncations, 2);
    }

    #[test]
    fn lsh_duplicate_buckets_emit_candidates_without_exact_digest() {
        let fingerprints = (0..4)
            .map(|id| (id, (id % 2) as ChainId, fingerprint(id as u8, false)))
            .collect::<Vec<_>>();
        let (pairs, stats) = generate_candidates(fingerprints, &config(), &NoopProgress).unwrap();
        assert!(!pairs.is_empty());
        assert_eq!(stats.exact_bucket_pairs, 0);
        assert!(stats.lsh_pairs > 0);
    }

    #[test]
    fn per_source_quota_limits_union_candidates() {
        let fingerprints = (0..6)
            .map(|id| (id, (id % 2) as ChainId, fingerprint(1, false)))
            .collect::<Vec<_>>();
        let mut config = config();
        config.max_outgoing_candidates_per_contract = 1;
        config.max_candidates_per_target_chain = 1;
        let (pairs, stats) = generate_candidates(fingerprints, &config, &NoopProgress).unwrap();
        assert!(!pairs.is_empty());
        assert!(stats.quota_truncations > 0);
    }
}
