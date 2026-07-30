use crate::entity::{ChainId, ContractId, EntityStore};
use ahash::{AHashMap, AHashSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::{BuildHasher, Hasher};

const IMPLICIT_EXHAUSTIVE_SAMPLE_MULTIPLIER: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicatePairSample {
    pub contract_a_chain: String,
    pub contract_a_address: String,
    pub contract_b_chain: String,
    pub contract_b_address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainDuplicatePairSamples {
    pub chain: String,
    pub pairs: Vec<DuplicatePairSample>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainPairDuplicatePairSamples {
    pub chain_a: String,
    pub chain_b: String,
    pub pairs: Vec<DuplicatePairSample>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DuplicatePairSamples {
    pub all_chains: Vec<DuplicatePairSample>,
    pub intra_chain: Vec<ChainDuplicatePairSamples>,
    pub chain_pairs: Vec<ChainPairDuplicatePairSamples>,
    pub cross_chain_summary: Vec<ChainDuplicatePairSamples>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ContractPair {
    left: ContractId,
    right: ContractId,
}

impl ContractPair {
    fn new(left: ContractId, right: ContractId) -> Option<Self> {
        (left != right).then(|| {
            let (left, right) = if left < right {
                (left, right)
            } else {
                (right, left)
            };
            Self { left, right }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SampleEntry {
    priority: u64,
    pair: ContractPair,
}

impl Ord for SampleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.pair.cmp(&other.pair))
    }
}

impl PartialOrd for SampleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PairSamplingPlan {
    capacity: usize,
    seed: u64,
    contract_chains: std::sync::Arc<[ChainId]>,
}

impl PairSamplingPlan {
    pub(crate) fn new(store: &EntityStore, capacity: usize) -> Self {
        let random = std::collections::hash_map::RandomState::new();
        let mut hasher = random.build_hasher();
        hasher.write_u64(capacity as u64);
        Self {
            capacity,
            seed: hasher.finish(),
            contract_chains: if capacity == 0 {
                std::sync::Arc::from([])
            } else {
                store
                    .contracts
                    .iter()
                    .map(|contract| contract.chain_id)
                    .collect::<Vec<_>>()
                    .into()
            },
        }
    }

    pub(crate) fn disabled() -> Self {
        Self {
            capacity: 0,
            seed: 0,
            contract_chains: std::sync::Arc::from([]),
        }
    }

    pub(crate) fn sampler(&self) -> PairSampler {
        PairSampler {
            capacity: self.capacity,
            seed: self.seed,
            contract_chains: self.contract_chains.clone(),
            all_chains: PairReservoir::default(),
            intra_chain: AHashMap::new(),
            chain_pairs: AHashMap::new(),
            cross_chain_summary: AHashMap::new(),
        }
    }
}

pub(crate) struct PairSampler {
    capacity: usize,
    seed: u64,
    contract_chains: std::sync::Arc<[ChainId]>,
    all_chains: PairReservoir,
    intra_chain: AHashMap<ChainId, PairReservoir>,
    chain_pairs: AHashMap<(ChainId, ChainId), PairReservoir>,
    cross_chain_summary: AHashMap<ChainId, PairReservoir>,
}

#[derive(Default)]
struct PairReservoir {
    heap: BinaryHeap<SampleEntry>,
    retained: AHashSet<ContractPair>,
}

impl PairSampler {
    pub(crate) fn enabled(&self) -> bool {
        self.capacity != 0
    }

    pub(crate) fn observe(&mut self, left: ContractId, right: ContractId) -> bool {
        if self.capacity == 0 {
            return false;
        }
        let Some(pair) = ContractPair::new(left, right) else {
            return false;
        };
        let entry = SampleEntry {
            priority: pair_priority(self.seed, pair),
            pair,
        };
        let changed = self.all_chains.observe(entry, self.capacity);
        let left_chain = self.contract_chains[left as usize];
        let right_chain = self.contract_chains[right as usize];
        if left_chain == right_chain {
            self.intra_chain
                .entry(left_chain)
                .or_default()
                .observe(entry, self.capacity);
        } else {
            let chain_pair = if left_chain < right_chain {
                (left_chain, right_chain)
            } else {
                (right_chain, left_chain)
            };
            self.chain_pairs
                .entry(chain_pair)
                .or_default()
                .observe(entry, self.capacity);
            self.cross_chain_summary
                .entry(left_chain)
                .or_default()
                .observe(entry, self.capacity);
            self.cross_chain_summary
                .entry(right_chain)
                .or_default()
                .observe(entry, self.capacity);
        }
        changed
    }

    /// Samples an implicit Cartesian product without enumerating large duplicate
    /// groups. Small groups are exhausted while the reservoir has room; large
    /// groups use bounded random probes and every confirmed group remains eligible.
    pub(crate) fn observe_cross_by<L, R>(
        &mut self,
        left_len: usize,
        right_len: usize,
        left_at: L,
        right_at: R,
        group_salt: u64,
    ) where
        L: Fn(usize) -> ContractId,
        R: Fn(usize) -> ContractId,
    {
        if !self.enabled() || left_len == 0 || right_len == 0 {
            return;
        }
        if left_len == 1 && right_len == 1 {
            self.observe(left_at(0), right_at(0));
            return;
        }
        let left_groups = self.group_members(left_len, &left_at);
        let right_groups = self.group_members(right_len, &right_at);
        let remaining = self.cross_group_remaining(&left_groups, &right_groups);
        let pair_count = left_len.saturating_mul(right_len);
        if remaining != 0
            && pair_count <= remaining.saturating_mul(IMPLICIT_EXHAUSTIVE_SAMPLE_MULTIPLIER)
        {
            for left in 0..left_len {
                for right in 0..right_len {
                    self.observe(left_at(left), right_at(right));
                }
            }
            return;
        }
        let target = remaining.max(1);
        let attempts = if remaining == 0 {
            1
        } else {
            target.saturating_mul(4).max(8)
        };
        for attempt in 0..attempts {
            let left = self.random_index(group_salt, attempt as u64 * 2, left_len);
            let right = self.random_index(group_salt, attempt as u64 * 2 + 1, right_len);
            self.observe(left_at(left), right_at(right));
        }
        self.top_up_cross_scopes(&left_groups, &right_groups, group_salt);
    }

    /// Samples an implicit complete graph without constructing its quadratic
    /// edge set. This is important for large identical Name/Metadata profiles.
    pub(crate) fn observe_clique_by<F>(&mut self, len: usize, at: F, group_salt: u64)
    where
        F: Fn(usize) -> ContractId,
    {
        if !self.enabled() || len < 2 {
            return;
        }
        let groups = self.group_members(len, &at);
        let remaining = self.clique_group_remaining(len, &groups);
        let pair_count = len.saturating_mul(len.saturating_sub(1)) / 2;
        if remaining != 0
            && pair_count <= remaining.saturating_mul(IMPLICIT_EXHAUSTIVE_SAMPLE_MULTIPLIER)
        {
            for left in 0..len - 1 {
                for right in left + 1..len {
                    self.observe(at(left), at(right));
                }
            }
            return;
        }
        let target = remaining.max(1);
        let attempts = if remaining == 0 {
            1
        } else {
            target.saturating_mul(6).max(12)
        };
        for attempt in 0..attempts {
            let left = self.random_index(group_salt, attempt as u64 * 2, len);
            let mut right = self.random_index(group_salt, attempt as u64 * 2 + 1, len - 1);
            if right >= left {
                right += 1;
            }
            self.observe(at(left), at(right));
        }
        self.top_up_clique_scopes(&groups, group_salt);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        debug_assert_eq!(self.capacity, other.capacity);
        debug_assert_eq!(self.seed, other.seed);
        self.all_chains.merge(other.all_chains, self.capacity);
        merge_reservoir_maps(&mut self.intra_chain, other.intra_chain, self.capacity);
        merge_reservoir_maps(&mut self.chain_pairs, other.chain_pairs, self.capacity);
        merge_reservoir_maps(
            &mut self.cross_chain_summary,
            other.cross_chain_summary,
            self.capacity,
        );
    }

    fn random_index(&self, group_salt: u64, ordinal: u64, len: usize) -> usize {
        debug_assert!(len != 0);
        splitmix64(self.seed ^ group_salt.rotate_left(17) ^ ordinal.wrapping_mul(MIX_GAMMA))
            as usize
            % len
    }

    fn reservoir_remaining<K: Eq + std::hash::Hash>(
        &self,
        reservoirs: &AHashMap<K, PairReservoir>,
        key: &K,
    ) -> usize {
        self.capacity.saturating_sub(
            reservoirs
                .get(key)
                .map_or(0, |reservoir| reservoir.heap.len()),
        )
    }

    fn group_members<F>(&self, len: usize, at: &F) -> AHashMap<ChainId, Vec<ContractId>>
    where
        F: Fn(usize) -> ContractId,
    {
        let mut groups = AHashMap::<ChainId, Vec<ContractId>>::new();
        for member in 0..len {
            let contract = at(member);
            groups
                .entry(self.contract_chains[contract as usize])
                .or_default()
                .push(contract);
        }
        groups
    }

    fn clique_group_remaining(
        &self,
        len: usize,
        groups: &AHashMap<ChainId, Vec<ContractId>>,
    ) -> usize {
        let mut remaining = self.capacity.saturating_sub(self.all_chains.heap.len());
        let chains = groups.keys().copied().collect::<Vec<_>>();
        for (&chain, members) in groups {
            if members.len() >= 2 {
                remaining = remaining.max(self.reservoir_remaining(&self.intra_chain, &chain));
            }
            if members.len() < len {
                remaining =
                    remaining.max(self.reservoir_remaining(&self.cross_chain_summary, &chain));
            }
        }
        for (offset, &left) in chains.iter().enumerate() {
            for &right in &chains[offset + 1..] {
                let pair = ordered_chain_pair(left, right);
                remaining = remaining.max(self.reservoir_remaining(&self.chain_pairs, &pair));
            }
        }
        remaining
    }

    fn cross_group_remaining(
        &self,
        left_groups: &AHashMap<ChainId, Vec<ContractId>>,
        right_groups: &AHashMap<ChainId, Vec<ContractId>>,
    ) -> usize {
        let mut remaining = self.capacity.saturating_sub(self.all_chains.heap.len());
        for &chain in left_groups.keys() {
            if right_groups.contains_key(&chain) {
                remaining = remaining.max(self.reservoir_remaining(&self.intra_chain, &chain));
            }
        }
        for &left in left_groups.keys() {
            for &right in right_groups.keys() {
                if left == right {
                    continue;
                }
                let pair = ordered_chain_pair(left, right);
                remaining = remaining.max(self.reservoir_remaining(&self.chain_pairs, &pair));
                remaining =
                    remaining.max(self.reservoir_remaining(&self.cross_chain_summary, &left));
                remaining =
                    remaining.max(self.reservoir_remaining(&self.cross_chain_summary, &right));
            }
        }
        remaining
    }

    fn top_up_clique_scopes(
        &mut self,
        groups: &AHashMap<ChainId, Vec<ContractId>>,
        group_salt: u64,
    ) {
        for (&chain, members) in groups {
            let remaining = self.reservoir_remaining(&self.intra_chain, &chain);
            if remaining != 0 && members.len() >= 2 {
                self.probe_clique(members, group_salt ^ u64::from(chain), remaining);
            }
        }
        let chains = groups.keys().copied().collect::<Vec<_>>();
        for (offset, &left) in chains.iter().enumerate() {
            for &right in &chains[offset + 1..] {
                let pair = ordered_chain_pair(left, right);
                let remaining = self.reservoir_remaining(&self.chain_pairs, &pair);
                if remaining != 0 {
                    self.probe_cross(
                        &groups[&left],
                        &groups[&right],
                        group_salt ^ (u64::from(left) << 16) ^ u64::from(right),
                        remaining,
                    );
                }
            }
        }
    }

    fn top_up_cross_scopes(
        &mut self,
        left_groups: &AHashMap<ChainId, Vec<ContractId>>,
        right_groups: &AHashMap<ChainId, Vec<ContractId>>,
        group_salt: u64,
    ) {
        for (&chain, left) in left_groups {
            if let Some(right) = right_groups.get(&chain) {
                let remaining = self.reservoir_remaining(&self.intra_chain, &chain);
                if remaining != 0 {
                    self.probe_cross(left, right, group_salt ^ u64::from(chain), remaining);
                }
            }
        }
        for (&left_chain, left) in left_groups {
            for (&right_chain, right) in right_groups {
                if left_chain == right_chain {
                    continue;
                }
                let pair = ordered_chain_pair(left_chain, right_chain);
                let remaining = self.reservoir_remaining(&self.chain_pairs, &pair);
                if remaining != 0 {
                    self.probe_cross(
                        left,
                        right,
                        group_salt ^ (u64::from(left_chain) << 16) ^ u64::from(right_chain),
                        remaining,
                    );
                }
            }
        }
    }

    fn probe_clique(&mut self, members: &[ContractId], salt: u64, remaining: usize) {
        let pair_count = members
            .len()
            .saturating_mul(members.len().saturating_sub(1))
            / 2;
        if pair_count <= remaining {
            for left in 0..members.len() - 1 {
                for right in left + 1..members.len() {
                    self.observe(members[left], members[right]);
                }
            }
            return;
        }
        for attempt in 0..remaining.saturating_mul(6).max(12) {
            let left = self.random_index(salt, attempt as u64 * 2, members.len());
            let mut right = self.random_index(salt, attempt as u64 * 2 + 1, members.len() - 1);
            if right >= left {
                right += 1;
            }
            self.observe(members[left], members[right]);
        }
    }

    fn probe_cross(
        &mut self,
        left_members: &[ContractId],
        right_members: &[ContractId],
        salt: u64,
        remaining: usize,
    ) {
        let pair_count = left_members.len().saturating_mul(right_members.len());
        if pair_count <= remaining {
            for &left in left_members {
                for &right in right_members {
                    self.observe(left, right);
                }
            }
            return;
        }
        for attempt in 0..remaining.saturating_mul(4).max(8) {
            let left = self.random_index(salt, attempt as u64 * 2, left_members.len());
            let right = self.random_index(salt, attempt as u64 * 2 + 1, right_members.len());
            self.observe(left_members[left], right_members[right]);
        }
    }
}

fn ordered_chain_pair(left: ChainId, right: ChainId) -> (ChainId, ChainId) {
    if left < right {
        (left, right)
    } else {
        (right, left)
    }
}

impl PairReservoir {
    fn observe(&mut self, entry: SampleEntry, capacity: usize) -> bool {
        if self.retained.contains(&entry.pair) {
            return false;
        }
        if self.heap.capacity() == 0 {
            self.heap.reserve(capacity);
            self.retained.reserve(capacity);
        }
        if self.heap.len() == capacity && self.heap.peek().is_some_and(|current| entry >= *current)
        {
            return false;
        }
        if self.heap.len() == capacity {
            let removed = self.heap.pop().expect("a full sample heap is non-empty");
            self.retained.remove(&removed.pair);
        }
        self.heap.push(entry);
        self.retained.insert(entry.pair);
        true
    }

    fn merge(&mut self, other: Self, capacity: usize) {
        for entry in other.heap {
            self.observe(entry, capacity);
        }
    }

    fn into_pairs(self) -> Vec<ContractPair> {
        let mut pairs = self
            .heap
            .into_iter()
            .map(|entry| entry.pair)
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs
    }
}

fn merge_reservoir_maps<K: Eq + std::hash::Hash>(
    target: &mut AHashMap<K, PairReservoir>,
    source: AHashMap<K, PairReservoir>,
    capacity: usize,
) {
    for (key, reservoir) in source {
        target.entry(key).or_default().merge(reservoir, capacity);
    }
}

const MIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(MIX_GAMMA);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn pair_priority(seed: u64, pair: ContractPair) -> u64 {
    let packed = (u64::from(pair.left) << 32) | u64::from(pair.right);
    splitmix64(seed ^ packed)
}

pub(crate) fn materialize_samples(
    store: &EntityStore,
    sampler: PairSampler,
) -> DuplicatePairSamples {
    fn materialize_pairs(
        store: &EntityStore,
        pairs: Vec<ContractPair>,
    ) -> Vec<DuplicatePairSample> {
        pairs
            .into_iter()
            .map(|pair| {
                let left = &store.contracts[pair.left as usize];
                let right = &store.contracts[pair.right as usize];
                DuplicatePairSample {
                    contract_a_chain: store.chain_name(left.chain_id).to_owned(),
                    contract_a_address: left.address.clone(),
                    contract_b_chain: store.chain_name(right.chain_id).to_owned(),
                    contract_b_address: right.address.clone(),
                }
            })
            .collect()
    }

    let mut intra_chain = sampler.intra_chain.into_iter().collect::<Vec<_>>();
    intra_chain.sort_unstable_by_key(|(chain, _)| *chain);
    let mut chain_pairs = sampler.chain_pairs.into_iter().collect::<Vec<_>>();
    chain_pairs.sort_unstable_by_key(|(chains, _)| *chains);
    let mut cross_chain_summary = sampler.cross_chain_summary.into_iter().collect::<Vec<_>>();
    cross_chain_summary.sort_unstable_by_key(|(chain, _)| *chain);

    DuplicatePairSamples {
        all_chains: materialize_pairs(store, sampler.all_chains.into_pairs()),
        intra_chain: intra_chain
            .into_iter()
            .map(|(chain, reservoir)| ChainDuplicatePairSamples {
                chain: store.chain_name(chain).to_owned(),
                pairs: materialize_pairs(store, reservoir.into_pairs()),
            })
            .collect(),
        chain_pairs: chain_pairs
            .into_iter()
            .map(
                |((chain_a, chain_b), reservoir)| ChainPairDuplicatePairSamples {
                    chain_a: store.chain_name(chain_a).to_owned(),
                    chain_b: store.chain_name(chain_b).to_owned(),
                    pairs: materialize_pairs(store, reservoir.into_pairs()),
                },
            )
            .collect(),
        cross_chain_summary: cross_chain_summary
            .into_iter()
            .map(|(chain, reservoir)| ChainDuplicatePairSamples {
                chain: store.chain_name(chain).to_owned(),
                pairs: materialize_pairs(store, reservoir.into_pairs()),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(capacity: usize) -> PairSamplingPlan {
        PairSamplingPlan {
            capacity,
            seed: 7,
            contract_chains: vec![0; 30_000].into(),
        }
    }

    #[test]
    fn keeps_a_bounded_unique_sample() {
        let mut sampler = plan(3).sampler();
        for left in 0..10 {
            sampler.observe(left, left + 100);
            sampler.observe(left + 100, left);
        }
        assert_eq!(sampler.all_chains.heap.len(), 3);
        assert_eq!(sampler.all_chains.retained.len(), 3);
    }

    #[test]
    fn merged_worker_samples_equal_a_single_sample() {
        let plan = plan(5);
        let mut all = plan.sampler();
        let mut even = plan.sampler();
        let mut odd = plan.sampler();
        for left in 0..100 {
            all.observe(left, left + 1000);
            if left % 2 == 0 {
                even.observe(left, left + 1000);
            } else {
                odd.observe(left, left + 1000);
            }
        }
        even.merge(odd);
        assert_eq!(all.all_chains.into_pairs(), even.all_chains.into_pairs());
    }

    #[test]
    fn singleton_cross_group_matches_direct_observation() {
        let plan = plan(5);
        let mut grouped = plan.sampler();
        let mut direct = plan.sampler();

        grouped.observe_cross_by(1, 1, |_| 7, |_| 11, 123);
        direct.observe(7, 11);

        assert_eq!(
            grouped.all_chains.into_pairs(),
            direct.all_chains.into_pairs()
        );
        assert_eq!(grouped.intra_chain[&0].heap.len(), 1);
        assert_eq!(direct.intra_chain[&0].heap.len(), 1);
    }

    #[test]
    fn implicit_large_group_stays_bounded_and_fills_the_sample() {
        let mut sampler = plan(25).sampler();
        sampler.observe_cross_by(10_000, 10_000, |id| id as u32, |id| id as u32 + 20_000, 9);
        assert_eq!(sampler.all_chains.heap.len(), 25);
    }

    #[test]
    fn near_capacity_implicit_group_is_exhausted_and_fills_the_sample() {
        let mut sampler = plan(1_000).sampler();
        sampler.observe_cross_by(1, 1_001, |_| 0, |id| id as u32 + 1, 29);
        assert_eq!(sampler.all_chains.heap.len(), 1_000);
        assert_eq!(sampler.intra_chain[&0].heap.len(), 1_000);
    }

    #[test]
    fn routes_pairs_to_every_requested_report_scope() {
        let plan = PairSamplingPlan {
            capacity: 10,
            seed: 7,
            contract_chains: vec![0, 0, 1, 2].into(),
        };
        let mut sampler = plan.sampler();
        sampler.observe(0, 1);
        sampler.observe(0, 2);
        sampler.observe(2, 3);

        assert_eq!(sampler.all_chains.heap.len(), 3);
        assert_eq!(sampler.intra_chain[&0].heap.len(), 1);
        assert_eq!(sampler.chain_pairs[&(0, 1)].heap.len(), 1);
        assert_eq!(sampler.chain_pairs[&(1, 2)].heap.len(), 1);
        assert_eq!(sampler.cross_chain_summary[&0].heap.len(), 1);
        assert_eq!(sampler.cross_chain_summary[&1].heap.len(), 2);
        assert_eq!(sampler.cross_chain_summary[&2].heap.len(), 1);
    }

    #[test]
    fn rare_chain_in_a_large_clique_fills_its_cross_chain_scopes() {
        let mut chains = vec![0; 10_001];
        chains[10_000] = 1;
        let plan = PairSamplingPlan {
            capacity: 25,
            seed: 7,
            contract_chains: chains.into(),
        };
        let mut sampler = plan.sampler();
        sampler.observe_clique_by(10_001, |member| member as ContractId, 19);

        assert_eq!(sampler.chain_pairs[&(0, 1)].heap.len(), 25);
        assert_eq!(sampler.cross_chain_summary[&0].heap.len(), 25);
        assert_eq!(sampler.cross_chain_summary[&1].heap.len(), 25);
    }
}
