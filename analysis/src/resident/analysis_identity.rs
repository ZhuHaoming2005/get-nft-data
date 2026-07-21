use crate::error::{AnalysisError, Result};
use crate::model::{ChainId, GlobalAddressId, GlobalNftId, GlobalTxId, NftKey};
use ahash::AHashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

const SHARDS: usize = 256;

#[derive(Debug, Default)]
struct IdentityShard {
    addresses: AHashMap<(ChainId, Arc<str>), GlobalAddressId>,
    transactions: AHashMap<(ChainId, Arc<str>), GlobalTxId>,
    nfts: AHashMap<NftKey, GlobalNftId>,
}

#[derive(Debug)]
pub struct AnalysisIdentityStore {
    shards: Vec<Mutex<IdentityShard>>,
    next_address: AtomicU32,
    next_transaction: AtomicU32,
    next_nft: AtomicU32,
}

impl Default for AnalysisIdentityStore {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| Mutex::new(IdentityShard::default()))
                .collect(),
            next_address: AtomicU32::new(0),
            next_transaction: AtomicU32::new(0),
            next_nft: AtomicU32::new(0),
        }
    }
}

impl AnalysisIdentityStore {
    pub fn intern_batch(
        &self,
        addresses: &mut Vec<(ChainId, Arc<str>)>,
        transactions: &mut Vec<(ChainId, Arc<str>)>,
        nfts: &mut Vec<NftKey>,
    ) -> Result<InternedIdentities> {
        addresses.sort();
        addresses.dedup();
        transactions.sort();
        transactions.dedup();
        nfts.sort();
        nfts.dedup();
        let mut address_output = Vec::with_capacity(addresses.len());
        let mut transaction_output = Vec::with_capacity(transactions.len());
        let mut nft_output = Vec::with_capacity(nfts.len());
        let address_indices = sharded_indices(addresses);
        let transaction_indices = sharded_indices(transactions);
        let nft_indices = sharded_indices(nfts);
        for range in shard_ranges(&address_indices) {
            let shard_id = address_indices[range.start].0;
            let mut shard = self.shards[shard_id].lock();
            for &(_, index) in &address_indices[range] {
                let key = &addresses[index];
                let id = match shard.addresses.get(key) {
                    Some(&id) => id,
                    None => {
                        let id = GlobalAddressId(next_id(&self.next_address, "global addresses")?);
                        shard.addresses.insert(key.clone(), id);
                        id
                    }
                };
                address_output.push((key.clone(), id));
            }
        }
        for range in shard_ranges(&transaction_indices) {
            let shard_id = transaction_indices[range.start].0;
            let mut shard = self.shards[shard_id].lock();
            for &(_, index) in &transaction_indices[range] {
                let key = &transactions[index];
                let id = match shard.transactions.get(key) {
                    Some(&id) => id,
                    None => {
                        let id =
                            GlobalTxId(next_id(&self.next_transaction, "global transactions")?);
                        shard.transactions.insert(key.clone(), id);
                        id
                    }
                };
                transaction_output.push((key.clone(), id));
            }
        }
        for range in shard_ranges(&nft_indices) {
            let shard_id = nft_indices[range.start].0;
            let mut shard = self.shards[shard_id].lock();
            for &(_, index) in &nft_indices[range] {
                let key = &nfts[index];
                let id = match shard.nfts.get(key) {
                    Some(&id) => id,
                    None => {
                        let id = GlobalNftId(next_id(&self.next_nft, "global NFTs")?);
                        shard.nfts.insert(key.clone(), id);
                        id
                    }
                };
                nft_output.push((key.clone(), id));
            }
        }
        address_output.sort_by(|left, right| left.0.cmp(&right.0));
        transaction_output.sort_by(|left, right| left.0.cmp(&right.0));
        nft_output.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(InternedIdentities {
            addresses: address_output,
            transactions: transaction_output,
            nfts: nft_output,
        })
    }

    pub fn counts(&self) -> AnalysisIdentityCounts {
        AnalysisIdentityCounts {
            addresses: self.next_address.load(Ordering::Relaxed),
            transactions: self.next_transaction.load(Ordering::Relaxed),
            nfts: self.next_nft.load(Ordering::Relaxed),
        }
    }
}

fn sharded_indices<T: std::hash::Hash>(values: &[T]) -> Vec<(usize, usize)> {
    let mut indices = values
        .iter()
        .enumerate()
        .map(|(index, value)| (identity_shard(value), index))
        .collect::<Vec<_>>();
    indices.sort_unstable();
    indices
}

fn shard_ranges(indices: &[(usize, usize)]) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < indices.len() {
        let shard = indices[start].0;
        let mut end = start + 1;
        while end < indices.len() && indices[end].0 == shard {
            end += 1;
        }
        ranges.push(start..end);
        start = end;
    }
    ranges
}

#[derive(Debug)]
pub struct InternedIdentities {
    pub addresses: Vec<((ChainId, Arc<str>), GlobalAddressId)>,
    pub transactions: Vec<((ChainId, Arc<str>), GlobalTxId)>,
    pub nfts: Vec<(NftKey, GlobalNftId)>,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct AnalysisIdentityCounts {
    pub addresses: u32,
    pub transactions: u32,
    pub nfts: u32,
}

fn identity_shard<T: std::hash::Hash>(value: &T) -> usize {
    static HASHER: OnceLock<ahash::RandomState> = OnceLock::new();
    let state = HASHER.get_or_init(|| ahash::RandomState::with_seeds(1, 2, 3, 4));
    state.hash_one(value) as usize & (SHARDS - 1)
}

fn next_id(counter: &AtomicU32, kind: &'static str) -> Result<u32> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            (value != u32::MAX).then_some(value + 1)
        })
        .map_err(|_| AnalysisError::IdCapacity {
            kind,
            count: u32::MAX as u64 + 1,
        })
}
