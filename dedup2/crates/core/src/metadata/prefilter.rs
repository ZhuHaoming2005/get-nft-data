use crate::entity::ContractId;
use crate::metadata::template::TemplateFingerprint;
use ahash::{AHashMap, AHashSet};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CandidatePair {
    pub left: ContractId,
    pub right: ContractId,
}

impl CandidatePair {
    pub fn new(a: ContractId, b: ContractId) -> Option<Self> {
        (a != b).then(|| {
            if a < b {
                Self { left: a, right: b }
            } else {
                Self { left: b, right: a }
            }
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
            // Target ~0.95 collision probability at t_tmpl via 1-(1-s^r)^b
            let t = self.template_jaccard_threshold.clamp(0.5, 0.99);
            let r = 2_u32;
            let mut b = 1_u32;
            while b < 64 {
                let p = 1.0 - (1.0 - t.powi(r as i32)).powi(b as i32);
                if p >= 0.95 {
                    break;
                }
                b += 1;
            }
            self.lsh_bands = b;
            self.lsh_rows_per_band = r;
        }
    }

    pub fn predicted_recall(&self) -> f64 {
        let t = self.template_jaccard_threshold.clamp(0.5, 0.99);
        let r = self.lsh_rows_per_band.max(1) as i32;
        let b = self.lsh_bands.max(1) as i32;
        1.0 - (1.0 - t.powi(r)).powi(b)
    }
}

pub fn generate_candidates(
    fingerprints: &[(ContractId, crate::entity::ChainId, TemplateFingerprint)],
    config: &PrefilterConfig,
) -> (Vec<CandidatePair>, PrefilterStats) {
    let mut stats = PrefilterStats {
        template_jaccard_threshold: config.template_jaccard_threshold,
        lsh_bands: config.lsh_bands,
        lsh_rows_per_band: config.lsh_rows_per_band,
        predicted_template_jaccard_recall: config.predicted_recall(),
        ..PrefilterStats::default()
    };

    let eligible: Vec<&(ContractId, crate::entity::ChainId, TemplateFingerprint)> = fingerprints
        .iter()
        .filter(|(_, _, fp)| {
            if fp.low_information {
                stats.low_information_contracts += 1;
                false
            } else {
                stats.eligible_contracts += 1;
                true
            }
        })
        .collect();

    let mut evidence: AHashMap<CandidatePair, (bool, u32, u32)> = AHashMap::new();

    // Exact digest buckets
    let mut buckets: AHashMap<[u8; 32], Vec<(ContractId, crate::entity::ChainId)>> = AHashMap::new();
    for (cid, chain, fp) in &eligible {
        buckets
            .entry(fp.digest)
            .or_default()
            .push((*cid, *chain));
    }
    for members in buckets.values_mut() {
        if members.len() < 2 {
            continue;
        }
        // Stable reducer order: ContractId ascending.
        members.sort_by_key(|(cid, _)| *cid);
        let mut emitted = 0_usize;
        'pairs: for i in 0..members.len() {
            for j in (i + 1)..members.len() {
                if emitted >= config.bucket_pair_cap {
                    stats.bucket_cap_truncations += 1;
                    break 'pairs;
                }
                if let Some(pair) = CandidatePair::new(members[i].0, members[j].0) {
                    let entry = evidence.entry(pair).or_insert((false, 0, 0));
                    entry.0 = true;
                    stats.exact_bucket_pairs += 1;
                    emitted += 1;
                }
            }
        }
    }

    // MinHash/LSH: intra-chain adjacent; inter-chain ordered merge-neighbors.
    let band_size = config.lsh_rows_per_band.max(1) as usize;
    let bands = config.lsh_bands.max(1) as usize;
    let num_hashes = band_size * bands;
    let mut band_buckets: Vec<AHashMap<u64, Vec<(ContractId, crate::entity::ChainId)>>> =
        vec![AHashMap::new(); bands];

    for (cid, chain, fp) in &eligible {
        let sig = minhash_signature(&fp.features, num_hashes);
        for band in 0..bands {
            let start = band * band_size;
            let mut h = 0xcbf29ce484222325_u64;
            for &part in &sig[start..start + band_size] {
                h ^= part as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            band_buckets[band]
                .entry(h)
                .or_default()
                .push((*cid, *chain));
        }
    }
    for band in &band_buckets {
        for members in band.values() {
            if members.len() < 2 {
                continue;
            }
            let mut by_chain: AHashMap<crate::entity::ChainId, Vec<ContractId>> = AHashMap::new();
            for &(cid, chain) in members {
                by_chain.entry(chain).or_default().push(cid);
            }
            for list in by_chain.values_mut() {
                list.sort_unstable();
                list.dedup();
                for window in list.windows(2) {
                    if let Some(pair) = CandidatePair::new(window[0], window[1]) {
                        let entry = evidence.entry(pair).or_insert((false, 0, 0));
                        entry.2 = entry.2.saturating_add(1);
                        if !entry.0 {
                            stats.lsh_pairs += 1;
                        }
                    }
                }
            }
            let mut chain_ids: Vec<_> = by_chain.keys().copied().collect();
            chain_ids.sort_unstable();
            let n = config.neighbors_per_target_chain.max(1);
            for (i, &ca) in chain_ids.iter().enumerate() {
                for &cb in &chain_ids[i + 1..] {
                    let left = &by_chain[&ca];
                    let right = &by_chain[&cb];
                    emit_inter_neighbors(left, right, n, &mut evidence, &mut stats);
                    emit_inter_neighbors(right, left, n, &mut evidence, &mut stats);
                }
            }
        }
    }

    // Fill shared feature counts
    let fp_by_id: AHashMap<ContractId, &TemplateFingerprint> = eligible
        .iter()
        .map(|(cid, _, fp)| (*cid, fp))
        .collect();
    let chain_by_id: AHashMap<ContractId, crate::entity::ChainId> = eligible
        .iter()
        .map(|(cid, chain, _)| (*cid, *chain))
        .collect();

    for (pair, entry) in evidence.iter_mut() {
        let Some(l) = fp_by_id.get(&pair.left) else {
            continue;
        };
        let Some(r) = fp_by_id.get(&pair.right) else {
            continue;
        };
        entry.1 = shared_feature_count(&l.features, &r.features);
    }

    // Per-contract quotas
    let mut ranked: Vec<(CandidatePair, bool, u32, u32)> = evidence
        .into_iter()
        .map(|(pair, (exact, shared, bands_hit))| (pair, exact, shared, bands_hit))
        .collect();
    ranked.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then(b.2.cmp(&a.2))
            .then(b.3.cmp(&a.3))
            .then(a.0.cmp(&b.0))
    });

    let mut kept: AHashSet<CandidatePair> = AHashSet::new();
    let mut outgoing: AHashMap<ContractId, usize> = AHashMap::new();
    let mut per_chain: AHashMap<(ContractId, crate::entity::ChainId), usize> = AHashMap::new();

    for (pair, _, _, _) in ranked {
        let left_chain = chain_by_id[&pair.left];
        let right_chain = chain_by_id[&pair.right];
        let ok_left = *outgoing.get(&pair.left).unwrap_or(&0)
            < config.max_outgoing_candidates_per_contract
            && *per_chain
                .get(&(pair.left, right_chain))
                .unwrap_or(&0)
                < config.max_candidates_per_target_chain;
        let ok_right = *outgoing.get(&pair.right).unwrap_or(&0)
            < config.max_outgoing_candidates_per_contract
            && *per_chain
                .get(&(pair.right, left_chain))
                .unwrap_or(&0)
                < config.max_candidates_per_target_chain;
        if !(ok_left || ok_right) {
            stats.quota_truncations += 1;
            continue;
        }
        if kept.insert(pair) {
            if ok_left {
                *outgoing.entry(pair.left).or_default() += 1;
                *per_chain.entry((pair.left, right_chain)).or_default() += 1;
            }
            if ok_right {
                *outgoing.entry(pair.right).or_default() += 1;
                *per_chain.entry((pair.right, left_chain)).or_default() += 1;
            }
        }
    }

    let mut out: Vec<CandidatePair> = kept.into_iter().collect();
    out.sort();
    (out, stats)
}

fn emit_inter_neighbors(
    left: &[ContractId],
    right: &[ContractId],
    neighbors: usize,
    evidence: &mut AHashMap<CandidatePair, (bool, u32, u32)>,
    stats: &mut PrefilterStats,
) {
    for &lc in left {
        let idx = right.partition_point(|&r| r < lc);
        let mut taken = 0_usize;
        for &rc in right.iter().skip(idx) {
            if taken >= neighbors {
                break;
            }
            if let Some(pair) = CandidatePair::new(lc, rc) {
                let entry = evidence.entry(pair).or_insert((false, 0, 0));
                entry.2 = entry.2.saturating_add(1);
                if !entry.0 {
                    stats.lsh_pairs += 1;
                }
            }
            taken += 1;
        }
        for &rc in right[..idx].iter().rev() {
            if taken >= neighbors {
                break;
            }
            if let Some(pair) = CandidatePair::new(lc, rc) {
                let entry = evidence.entry(pair).or_insert((false, 0, 0));
                entry.2 = entry.2.saturating_add(1);
                if !entry.0 {
                    stats.lsh_pairs += 1;
                }
            }
            taken += 1;
        }
    }
}

fn shared_feature_count(left: &[String], right: &[String]) -> u32 {
    let mut i = 0;
    let mut j = 0;
    let mut n = 0_u32;
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                n += 1;
                i += 1;
                j += 1;
            }
        }
    }
    n
}

fn minhash_signature(features: &[String], num_hashes: usize) -> Vec<u32> {
    let mut sig = vec![u32::MAX; num_hashes];
    if features.is_empty() {
        return sig;
    }
    for feature in features {
        let mut h = 0x811c9dc5_u32;
        for byte in feature.as_bytes() {
            h ^= u32::from(*byte);
            h = h.wrapping_mul(0x01000193);
        }
        for i in 0..num_hashes {
            let hi = h
                .wrapping_mul(0x85ebca77)
                .wrapping_add((i as u32).wrapping_mul(0xc2b2ae3d));
            if hi < sig[i] {
                sig[i] = hi;
            }
        }
    }
    sig
}
