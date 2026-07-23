//! Compact sparse-row posting index.

use super::ids::{ContractId, NftId, StringId};

/// CSR postings: sorted keys with contiguous value slices via offsets.
///
/// Layout: `values[offsets[i]..offsets[i + 1]]` belongs to `keys[i]`.
#[derive(Clone, Debug, Default)]
pub struct CsrIndex {
    pub keys: Vec<u32>,
    pub offsets: Vec<u32>,
    pub values: Vec<u32>,
}

impl CsrIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Binary-search `key` and return its value slice, if present.
    pub fn values_for(&self, key: u32) -> Option<&[u32]> {
        let index = self.keys.binary_search(&key).ok()?;
        let start = self.offsets.get(index).copied()? as usize;
        let end = self.offsets.get(index + 1).copied()? as usize;
        self.values.get(start..end)
    }

    /// Build CSR from sorted `(key, value)` pairs (already grouped by key).
    pub fn from_sorted_pairs(pairs: &[(u32, u32)]) -> Self {
        if pairs.is_empty() {
            return Self {
                keys: Vec::new(),
                offsets: vec![0],
                values: Vec::new(),
            };
        }
        let mut keys = Vec::new();
        let mut offsets = vec![0_u32];
        let mut values = Vec::with_capacity(pairs.len());
        let mut index = 0;
        while index < pairs.len() {
            let key = pairs[index].0;
            let start = index;
            index += 1;
            while index < pairs.len() && pairs[index].0 == key {
                index += 1;
            }
            keys.push(key);
            for &(_, value) in &pairs[start..index] {
                values.push(value);
            }
            offsets.push(values.len() as u32);
        }
        Self {
            keys,
            offsets,
            values,
        }
    }
}

/// URI posting group identity (contract-scoped), for future builders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UriPostingKey {
    pub uri_id: StringId,
    pub contract_id: ContractId,
}

/// Name posting payload stub (contract- or NFT-level members).
#[derive(Clone, Debug, Default)]
pub struct NamePostingStub {
    pub name_id: Option<StringId>,
    pub contract_ids: Vec<ContractId>,
    pub nft_ids: Vec<NftId>,
}
