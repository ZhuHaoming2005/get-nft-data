use crate::entity::{ChainId, ContractId, Dimension, EntityStore, ScopeKind};
use crate::scope::{ScopeCounts, ScopeKey};
use ahash::{AHashMap, AHashSet};

#[derive(Clone, Debug, Default)]
pub struct SummaryAccumulator {
    /// Objects already counted in a scope (contract-level or nft-row keyed).
    counted_contracts: AHashMap<ScopeKey, AHashSet<ContractId>>,
    counts: AHashMap<ScopeKey, ScopeCounts>,
}

impl SummaryAccumulator {
    pub fn mark_contract_duplicate(
        &mut self,
        store: &EntityStore,
        contract_id: ContractId,
        dimension: Dimension,
        peer_chain: ChainId,
    ) {
        let contract = &store.contracts[contract_id as usize];
        let primary = contract.chain_id;
        if primary == peer_chain {
            self.mark_contract(store, contract_id, dimension, ScopeKind::IntraChain, None);
        } else {
            self.mark_contract(
                store,
                contract_id,
                dimension,
                ScopeKind::CrossChainSummary,
                None,
            );
            self.mark_contract(
                store,
                contract_id,
                dimension,
                ScopeKind::ChainMatrix,
                Some(peer_chain),
            );
        }
    }

    pub fn mark_contract(
        &mut self,
        store: &EntityStore,
        contract_id: ContractId,
        dimension: Dimension,
        kind: ScopeKind,
        secondary_chain: Option<ChainId>,
    ) {
        let contract = &store.contracts[contract_id as usize];
        let key = ScopeKey {
            kind,
            primary_chain: contract.chain_id,
            secondary_chain,
            dimension,
        };
        let set = self.counted_contracts.entry(key.clone()).or_default();
        if set.insert(contract_id) {
            self.counts
                .entry(key)
                .or_default()
                .add_contract(contract.nft_count);
        }
    }

    /// URI path: count NFT rows; contract once per scope.
    pub fn mark_uri_hit(
        &mut self,
        store: &EntityStore,
        contract_id: ContractId,
        nft_rows: u64,
        dimension: Dimension,
        peer_chain: ChainId,
    ) {
        let contract = &store.contracts[contract_id as usize];
        let primary = contract.chain_id;
        let scopes: &[(ScopeKind, Option<ChainId>)] = if primary == peer_chain {
            &[(ScopeKind::IntraChain, None)]
        } else {
            &[
                (ScopeKind::CrossChainSummary, None),
                (ScopeKind::ChainMatrix, Some(peer_chain)),
            ]
        };
        for &(kind, secondary) in scopes {
            let key = ScopeKey {
                kind,
                primary_chain: primary,
                secondary_chain: secondary,
                dimension,
            };
            let set = self.counted_contracts.entry(key.clone()).or_default();
            let first = set.insert(contract_id);
            let entry = self.counts.entry(key).or_default();
            if first {
                entry.duplicate_contract_count += 1;
            }
            entry.add_nfts(nft_rows);
        }
    }

    pub fn counts(&self) -> &AHashMap<ScopeKey, ScopeCounts> {
        &self.counts
    }
}
