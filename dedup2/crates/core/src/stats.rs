use crate::entity::{ChainId, ContractId, Dimension, EntityStore, NftId, ScopeKind, StringId};
use crate::scope::{ScopeCounts, ScopeKey};
use ahash::{AHashMap, AHashSet};

#[derive(Clone, Debug, Default)]
pub struct SummaryAccumulator {
    counted_contracts: AHashMap<ScopeKey, AHashSet<ContractId>>,
    counted_nfts: AHashMap<ScopeKey, AHashSet<NftId>>,
    /// URI units use interned integer IDs, avoiding one owned URI string per scope hit.
    counted_uri_units: AHashMap<ScopeKey, AHashSet<(ContractId, StringId)>>,
    counts: AHashMap<ScopeKey, ScopeCounts>,
}

impl SummaryAccumulator {
    pub(crate) fn merge_unique_contract_counts(&mut self, totals: AHashMap<ScopeKey, ScopeCounts>) {
        for (key, value) in totals {
            let target = self.counts.entry(key).or_default();
            target.duplicate_contract_count = target
                .duplicate_contract_count
                .saturating_add(value.duplicate_contract_count);
            target.duplicate_nft_count = target
                .duplicate_nft_count
                .saturating_add(value.duplicate_nft_count);
        }
    }

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

    pub fn mark_nft_duplicate(
        &mut self,
        store: &EntityStore,
        nft_id: NftId,
        dimension: Dimension,
        peer_chain: ChainId,
    ) {
        let nft = &store.nfts[nft_id as usize];
        let contract = &store.contracts[nft.contract_id as usize];
        if contract.chain_id == peer_chain {
            return;
        }
        for (kind, secondary_chain) in [
            (ScopeKind::CrossChainSummary, None),
            (ScopeKind::ChainMatrix, Some(peer_chain)),
        ] {
            let key = ScopeKey {
                kind,
                primary_chain: contract.chain_id,
                secondary_chain,
                dimension,
            };
            let entry = self.counts.entry(key.clone()).or_default();
            if self
                .counted_contracts
                .entry(key.clone())
                .or_default()
                .insert(contract.id)
            {
                entry.duplicate_contract_count += 1;
            }
            if self.counted_nfts.entry(key).or_default().insert(nft_id) {
                entry.duplicate_nft_count += 1;
            }
        }
    }

    /// URI path: each (contract, uri) unit once per scope; NFT rows from that unit.
    pub fn mark_uri_hit(
        &mut self,
        store: &EntityStore,
        contract_id: ContractId,
        uri_id: StringId,
        nft_rows: u64,
        dimension: Dimension,
        peer_chain: ChainId,
    ) {
        if nft_rows == 0 {
            return;
        }
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
            self.mark_uri_scope_hit(
                store,
                contract_id,
                uri_id,
                nft_rows,
                dimension,
                (kind, secondary),
            );
        }
    }

    pub fn mark_uri_scope_hit(
        &mut self,
        store: &EntityStore,
        contract_id: ContractId,
        uri_id: StringId,
        nft_rows: u64,
        dimension: Dimension,
        scope: (ScopeKind, Option<ChainId>),
    ) {
        if nft_rows == 0 {
            return;
        }
        let contract = &store.contracts[contract_id as usize];
        let key = ScopeKey {
            kind: scope.0,
            primary_chain: contract.chain_id,
            secondary_chain: scope.1,
            dimension,
        };
        let units = self.counted_uri_units.entry(key.clone()).or_default();
        if !units.insert((contract_id, uri_id)) {
            return;
        }
        let contracts = self.counted_contracts.entry(key.clone()).or_default();
        let entry = self.counts.entry(key).or_default();
        if contracts.insert(contract_id) {
            entry.duplicate_contract_count += 1;
        }
        entry.add_nfts(nft_rows);
    }

    pub fn counts(&self) -> &AHashMap<ScopeKey, ScopeCounts> {
        &self.counts
    }
}
