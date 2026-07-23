//! ResidentStore: identity layers, URI CSR, and metadata anchors.

use ahash::{AHashMap, AHashSet};
use rayon::prelude::*;

use crate::Analysis2Error;

use crate::dedup::metadata::MetadataIndex;

use super::csr::{CsrIndex, UriChainIndex};
use super::ids::{
    compare_token_ids_desc, ChainId, ChainTotals, Contract, ContractId, MetadataRecord, Nft, NftId,
    SourceOrder, StringId,
};
use super::string_pool::StringPool;

/// Fully-resident snapshot: identity + string pool + CSR indexes + anchors.
#[derive(Clone, Debug)]
pub struct ResidentStore {
    pub chains: Vec<String>,
    pub chain_ids: AHashMap<String, ChainId>,
    pub contracts: Vec<Contract>,
    /// `(chain, address_string_id)` → contract. Address text lives in `strings`.
    pub contract_index: AHashMap<(ChainId, StringId), ContractId>,
    pub nfts: Vec<Nft>,
    /// `(contract, token_id_string_id)` → nft. Token text lives in `strings` and on `Nft`.
    pub nft_index: AHashMap<(ContractId, StringId), NftId>,
    pub strings: StringPool,
    /// Contract id → resident NFT ids, used by seed-scoped queries.
    pub contract_nft_csr: CsrIndex,
    pub token_uri_csr: UriChainIndex,
    pub image_uri_csr: UriChainIndex,
    /// EVM contract-level name postings (filled by `finalize_name_index`).
    pub name_contract_csr: CsrIndex,
    /// Solana NFT-level name postings (filled by `finalize_name_index`).
    pub name_nft_csr: CsrIndex,
    /// Unique indexed name ids sorted by char length then text (JW length windows).
    pub name_keys_by_len: Vec<StringId>,
    /// Character lengths parallel to `name_keys_by_len` for allocation-free windows.
    pub name_key_char_lens: Vec<u32>,
    /// Offsets into `name_sorted_chars`, parallel to `name_keys_by_len`.
    pub name_sorted_char_offsets: Vec<u64>,
    /// Sorted Unicode scalar values for every indexed name, stored contiguously.
    pub name_sorted_chars: Vec<char>,
    /// Indexed name string id → index in `name_keys_by_len` (sparse; only name keys).
    pub name_key_positions: AHashMap<StringId, u32>,
    /// Offsets into `name_occurrence_tokens`, parallel to `name_keys_by_len`.
    pub name_occurrence_token_offsets: Vec<u64>,
    /// Per-name occurrence tokens ordered from rarest to most common.
    pub name_occurrence_tokens: Vec<u32>,
    /// Occurrence token → length-sorted name indexes.
    pub name_occurrence_postings: CsrIndex,
    /// Prepared BM25 documents + term postings (filled by `finalize_metadata_index`).
    pub metadata_index: MetadataIndex,
    pub totals: AHashMap<ChainId, ChainTotals>,
    pub rows_loaded: u64,
    metadata_anchor_limit: usize,
    evm_chains: AHashSet<String>,
}

impl Default for ResidentStore {
    fn default() -> Self {
        Self::with_options(8, &AHashSet::default())
    }
}

impl ResidentStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(metadata_anchor_limit: usize, evm_chains: &AHashSet<String>) -> Self {
        Self {
            chains: Vec::new(),
            chain_ids: AHashMap::new(),
            contracts: Vec::new(),
            contract_index: AHashMap::new(),
            nfts: Vec::new(),
            nft_index: AHashMap::new(),
            strings: StringPool::new(),
            contract_nft_csr: CsrIndex::new(),
            token_uri_csr: UriChainIndex::new(),
            image_uri_csr: UriChainIndex::new(),
            name_contract_csr: CsrIndex::new(),
            name_nft_csr: CsrIndex::new(),
            name_keys_by_len: Vec::new(),
            name_key_char_lens: Vec::new(),
            name_sorted_char_offsets: vec![0],
            name_sorted_chars: Vec::new(),
            name_key_positions: AHashMap::new(),
            name_occurrence_token_offsets: vec![0],
            name_occurrence_tokens: Vec::new(),
            name_occurrence_postings: CsrIndex::new(),
            metadata_index: MetadataIndex::default(),
            totals: AHashMap::new(),
            rows_loaded: 0,
            metadata_anchor_limit,
            evm_chains: evm_chains.clone(),
        }
    }

    pub fn metadata_anchor_limit(&self) -> usize {
        self.metadata_anchor_limit
    }

    pub fn chain_name(&self, id: ChainId) -> &str {
        &self.chains[id as usize]
    }

    pub fn string(&self, id: StringId) -> &str {
        self.strings.get(id)
    }

    pub fn string_id(&self, value: &str) -> Option<StringId> {
        self.strings.lookup(value)
    }

    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty() && self.nfts.is_empty() && self.strings.is_empty()
    }

    pub fn is_evm_chain(&self, chain: &str) -> bool {
        self.evm_chains.contains(chain)
    }

    /// Resolve `(chain, address)` without cloning into the index key.
    pub fn contract_id(&self, chain: &str, address: &str) -> Option<ContractId> {
        let chain_id = *self.chain_ids.get(chain)?;
        let address_id = self.strings.lookup(address)?;
        self.contract_index.get(&(chain_id, address_id)).copied()
    }

    pub fn ensure_chain(&mut self, chain: &str) -> Result<ChainId, Analysis2Error> {
        if let Some(id) = self.chain_ids.get(chain) {
            return Ok(*id);
        }
        let id = ChainId::try_from(self.chains.len())
            .map_err(|_| Analysis2Error::invalid("too many chains for ChainId"))?;
        self.chains.push(chain.to_owned());
        self.chain_ids.insert(chain.to_owned(), id);
        self.totals.insert(id, ChainTotals::default());
        Ok(id)
    }

    /// Pass-1 identity row (no metadata). Prefer [`Self::ingest_identity_strs`] on hot paths.
    pub fn ingest_identity_row(&mut self, row: IdentityRow) -> Result<(), Analysis2Error> {
        self.ingest_identity_strs(
            &row.chain,
            &row.contract_address,
            &row.token_id,
            &row.name_norm,
            &row.token_uri_norm,
            &row.image_uri_norm,
            row.source_order,
        )
    }

    /// Zero intermediate-`IdentityRow` ingest: intern from borrowed field slices.
    pub fn ingest_identity_strs(
        &mut self,
        chain: &str,
        contract_address: &str,
        token_id: &str,
        name_norm: &str,
        token_uri_norm: &str,
        image_uri_norm: &str,
        source_order: SourceOrder,
    ) -> Result<(), Analysis2Error> {
        if chain.is_empty() || contract_address.is_empty() || token_id.is_empty() {
            return Ok(());
        }
        let chain_id = self.ensure_chain(chain)?;
        let address_id = self.strings.intern(contract_address);
        let contract_key = (chain_id, address_id);
        let contract_id = if let Some(id) = self.contract_index.get(&contract_key).copied() {
            id
        } else {
            let id = ContractId::try_from(self.contracts.len())
                .map_err(|_| Analysis2Error::invalid("too many contracts for ContractId"))?;
            self.contracts.push(Contract {
                id,
                chain_id,
                address: self.strings.get(address_id).to_owned(),
                nft_count: 0,
                name_id: None,
                metadata_by_token: Vec::new(),
            });
            self.contract_index.insert(contract_key, id);
            self.totals.entry(chain_id).or_default().contracts += 1;
            id
        };

        let token_sid = self.strings.intern(token_id);
        let nft_key = (contract_id, token_sid);
        if let Some(&nft_id) = self.nft_index.get(&nft_key) {
            self.merge_duplicate_nft(nft_id, name_norm, token_uri_norm, image_uri_norm, chain)?;
            return Ok(());
        }

        let name_id = self.strings.intern_nonblank(name_norm);
        let token_uri_id = self.strings.intern_nonempty(token_uri_norm);
        let image_uri_id = self.strings.intern_nonempty(image_uri_norm);
        let nft_id = NftId::try_from(self.nfts.len())
            .map_err(|_| Analysis2Error::invalid("too many NFTs for NftId"))?;
        self.nfts.push(Nft {
            id: nft_id,
            contract_id,
            token_id: self.strings.get(token_sid).to_owned(),
            name_id,
            token_uri_id,
            image_uri_id,
            source_order,
        });
        self.nft_index.insert(nft_key, nft_id);
        self.contracts[contract_id as usize].nft_count += 1;
        self.totals.entry(chain_id).or_default().nfts += 1;
        self.rows_loaded += 1;
        Ok(())
    }

    fn merge_duplicate_nft(
        &mut self,
        nft_id: NftId,
        name_norm: &str,
        token_uri_norm: &str,
        image_uri_norm: &str,
        chain: &str,
    ) -> Result<(), Analysis2Error> {
        let existing = &self.nfts[nft_id as usize];
        let existing_name = existing.name_id;
        let existing_token = existing.token_uri_id;
        let existing_image = existing.image_uri_id;
        let token_id = existing.token_id.clone();

        let name_id = if existing_name.is_none() {
            self.strings.intern_nonblank(name_norm)
        } else {
            existing_name
        };
        let token_uri_id =
            self.merge_uri_value(existing_token, token_uri_norm, "token_uri_norm", chain, &token_id)?;
        let image_uri_id =
            self.merge_uri_value(existing_image, image_uri_norm, "image_uri_norm", chain, &token_id)?;

        let nft = &mut self.nfts[nft_id as usize];
        nft.name_id = name_id;
        nft.token_uri_id = token_uri_id;
        nft.image_uri_id = image_uri_id;
        Ok(())
    }

    fn merge_uri_value(
        &mut self,
        existing: Option<StringId>,
        incoming: &str,
        field: &str,
        chain: &str,
        token_id: &str,
    ) -> Result<Option<StringId>, Analysis2Error> {
        if incoming.trim().is_empty() {
            return Ok(existing);
        }
        if let Some(id) = existing {
            if self.string(id) != incoming {
                return Err(Analysis2Error::invalid(format!(
                    "snapshot conflict for ({chain}, token {token_id}): distinct {field} values",
                )));
            }
            return Ok(existing);
        }
        Ok(self.strings.intern_nonempty(incoming))
    }

    /// Pass-2 metadata anchor insert (descending token id, first k valid).
    pub fn ingest_metadata_anchor(
        &mut self,
        chain: &str,
        contract_address: &str,
        token_id: &str,
        json: String,
        canonical_json: String,
        source_order: SourceOrder,
    ) -> Result<(), Analysis2Error> {
        if self.metadata_anchor_limit == 0 {
            return Ok(());
        }
        let Some(&chain_id) = self.chain_ids.get(chain) else {
            return Ok(());
        };
        let Some(address_id) = self.strings.lookup(contract_address) else {
            return Ok(());
        };
        let Some(&contract_id) = self.contract_index.get(&(chain_id, address_id)) else {
            return Ok(());
        };
        self.insert_metadata_anchor(
            contract_id,
            chain,
            token_id.to_owned(),
            json,
            canonical_json,
            source_order,
        );
        Ok(())
    }

    fn insert_metadata_anchor(
        &mut self,
        contract_id: ContractId,
        chain: &str,
        token_id: String,
        json: String,
        canonical_json: String,
        source_order: SourceOrder,
    ) {
        if self.metadata_anchor_limit == 0 {
            return;
        }
        let is_evm = self.is_evm_chain(chain);
        let anchors = &mut self.contracts[contract_id as usize].metadata_by_token;
        // Same token id: keep first valid in source order.
        if anchors.iter().any(|record| record.token_id == token_id) {
            return;
        }
        let insert_at = anchors
            .binary_search_by(|record| compare_token_ids_desc(&record.token_id, &token_id, is_evm))
            .unwrap_or_else(|position| position);
        if insert_at >= self.metadata_anchor_limit && anchors.len() >= self.metadata_anchor_limit {
            return;
        }
        anchors.insert(
            insert_at,
            MetadataRecord {
                token_id,
                json,
                canonical_json,
                source_order,
            },
        );
        if anchors.len() > self.metadata_anchor_limit {
            anchors.pop();
        }
    }

    pub fn rebuild_uri_csr(&mut self) {
        let (contract_nft_csr, (token_uri_csr, image_uri_csr)) = rayon::join(
            || build_contract_nft_csr(&self.nfts),
            || {
                rayon::join(
                    || build_uri_chain_index(&self.nfts, &self.contracts, true),
                    || build_uri_chain_index(&self.nfts, &self.contracts, false),
                )
            },
        );
        self.contract_nft_csr = contract_nft_csr;
        self.token_uri_csr = token_uri_csr;
        self.image_uri_csr = image_uri_csr;
    }

    pub(crate) fn rebuild_contract_nft_csr(&mut self) {
        self.contract_nft_csr = build_contract_nft_csr(&self.nfts);
    }

    /// NFT ids for a contract (CSR slice; empty when missing).
    pub fn nfts_for_contract(&self, contract_id: ContractId) -> &[NftId] {
        self.contract_nft_csr
            .values_for(contract_id)
            .unwrap_or(&[])
    }

    /// Free URI indexes after the URI query stage.
    pub fn drop_uri_indexes(&mut self) {
        self.token_uri_csr.clear();
        self.image_uri_csr.clear();
    }

    /// Free Name indexes after the Name query stage.
    pub fn drop_name_indexes(&mut self) {
        self.name_contract_csr.clear();
        self.name_nft_csr.clear();
        self.name_keys_by_len.clear();
        self.name_keys_by_len.shrink_to_fit();
        self.name_key_char_lens.clear();
        self.name_key_char_lens.shrink_to_fit();
        self.name_sorted_char_offsets.clear();
        self.name_sorted_char_offsets.shrink_to_fit();
        self.name_sorted_chars.clear();
        self.name_sorted_chars.shrink_to_fit();
        self.name_key_positions.clear();
        self.name_key_positions.shrink_to_fit();
        self.name_occurrence_token_offsets.clear();
        self.name_occurrence_token_offsets.shrink_to_fit();
        self.name_occurrence_tokens.clear();
        self.name_occurrence_tokens.shrink_to_fit();
        self.name_occurrence_postings.clear();
    }

    /// Free Metadata BM25 index after the Metadata query stage.
    pub fn drop_metadata_index(&mut self) {
        self.metadata_index = MetadataIndex::default();
        // Anchors on contracts are only needed for BM25 finalize/query; release
        // the large JSON payloads before enrich/report.
        for contract in &mut self.contracts {
            contract.metadata_by_token.clear();
            contract.metadata_by_token.shrink_to_fit();
        }
    }

    /// Merge another shard; preserves destination (left) identity and remaps shard ids.
    pub fn merge_shard(&mut self, shard: ResidentStore) -> Result<(), Analysis2Error> {
        let ResidentStore {
            chains,
            contracts,
            nfts,
            strings,
            ..
        } = shard;

        self.chains.reserve(chains.len());
        self.chain_ids.reserve(chains.len());
        self.contracts.reserve(contracts.len());
        self.contract_index.reserve(contracts.len());
        self.nfts.reserve(nfts.len());
        self.nft_index.reserve(nfts.len());
        self.strings.reserve(strings.len());
        let mut chain_map = Vec::with_capacity(chains.len());
        for chain in &chains {
            chain_map.push(self.ensure_chain(chain)?);
        }
        let mut string_map = Vec::with_capacity(strings.len());
        for i in 0..strings.len() {
            string_map.push(self.strings.intern(strings.get(i as StringId)));
        }

        let mut contract_map = vec![0; contracts.len()];
        for contract in contracts {
            let chain_id = chain_map[contract.chain_id as usize];
            let address_id = self.strings.intern(&contract.address);
            let key = (chain_id, address_id);
            let contract_id = if let Some(&existing) = self.contract_index.get(&key) {
                existing
            } else {
                let id = ContractId::try_from(self.contracts.len())
                    .map_err(|_| Analysis2Error::invalid("too many contracts for ContractId"))?;
                self.contracts.push(Contract {
                    id,
                    chain_id,
                    address: contract.address.clone(),
                    nft_count: 0,
                    name_id: None,
                    metadata_by_token: Vec::new(),
                });
                self.contract_index.insert(key, id);
                self.totals.entry(chain_id).or_default().contracts += 1;
                id
            };
            contract_map[contract.id as usize] = contract_id;
            let chain_name = self.chains[chain_id as usize].clone();
            for record in contract.metadata_by_token {
                self.insert_metadata_anchor(
                    contract_id,
                    &chain_name,
                    record.token_id,
                    record.json,
                    record.canonical_json,
                    record.source_order,
                );
            }
        }

        for nft in nfts {
            let contract_id = contract_map[nft.contract_id as usize];
            let name_id = nft.name_id.map(|id| string_map[id as usize]);
            let token_uri_id = nft.token_uri_id.map(|id| string_map[id as usize]);
            let image_uri_id = nft.image_uri_id.map(|id| string_map[id as usize]);
            let token_sid = self.strings.intern(&nft.token_id);
            let nft_key = (contract_id, token_sid);
            if let Some(&existing_id) = self.nft_index.get(&nft_key) {
                let existing = &mut self.nfts[existing_id as usize];
                if existing.name_id.is_none() {
                    existing.name_id = name_id;
                }
                existing.token_uri_id = merge_mapped_uri(
                    existing.token_uri_id,
                    token_uri_id,
                    "token_uri_norm",
                    contract_id,
                    &nft.token_id,
                )?;
                existing.image_uri_id = merge_mapped_uri(
                    existing.image_uri_id,
                    image_uri_id,
                    "image_uri_norm",
                    contract_id,
                    &nft.token_id,
                )?;
                continue;
            }
            let nft_id = NftId::try_from(self.nfts.len())
                .map_err(|_| Analysis2Error::invalid("too many NFTs for NftId"))?;
            self.nfts.push(Nft {
                id: nft_id,
                contract_id,
                token_id: nft.token_id.clone(),
                name_id,
                token_uri_id,
                image_uri_id,
                source_order: nft.source_order,
            });
            self.nft_index.insert(nft_key, nft_id);
            let chain_id = self.contracts[contract_id as usize].chain_id;
            self.contracts[contract_id as usize].nft_count += 1;
            self.totals.entry(chain_id).or_default().nfts += 1;
            self.rows_loaded += 1;
        }
        Ok(())
    }
}

/// Pass-1 decoded row (identity + name + URI; no metadata).
#[derive(Clone, Debug)]
pub struct IdentityRow {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name_norm: String,
    pub token_uri_norm: String,
    pub image_uri_norm: String,
    pub source_order: SourceOrder,
}

fn merge_mapped_uri(
    existing: Option<StringId>,
    incoming: Option<StringId>,
    field: &str,
    contract_id: ContractId,
    token_id: &str,
) -> Result<Option<StringId>, Analysis2Error> {
    match (existing, incoming) {
        (None, value) => Ok(value),
        (value, None) => Ok(value),
        (Some(existing), Some(incoming)) if existing == incoming => Ok(Some(existing)),
        (Some(_), Some(_)) => Err(Analysis2Error::invalid(format!(
            "snapshot conflict for contract {contract_id}, token {token_id}: distinct {field} values"
        ))),
    }
}

fn build_uri_chain_index(
    nfts: &[Nft],
    contracts: &[Contract],
    token_dimension: bool,
) -> UriChainIndex {
    let mut triples: Vec<(u32, u16, u32)> = nfts
        .par_iter()
        .filter_map(|nft| {
            let uri = if token_dimension {
                nft.token_uri_id
            } else {
                nft.image_uri_id
            }?;
            let chain = contracts.get(nft.contract_id as usize)?.chain_id;
            Some((uri, chain, nft.id))
        })
        .collect();
    triples.par_sort_unstable();
    UriChainIndex::from_sorted_triples(&triples)
}

fn build_contract_nft_csr(nfts: &[Nft]) -> CsrIndex {
    let mut pairs: Vec<(u32, u32)> = nfts
        .par_iter()
        .map(|nft| (nft.contract_id, nft.id))
        .collect();
    pairs.par_sort_unstable();
    CsrIndex::from_sorted_pairs(&pairs)
}

/// Deprecated alias kept for call-site stability; prefer [`crate::dedup::name::finalize_name_index`].
pub fn finalize_name_representatives_stub(store: &mut ResidentStore) {
    let _ = store; // real finalize is invoked from the load path via dedup::name
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashSet;

    #[test]
    fn resident_store_default_is_empty_skeleton() {
        let store = ResidentStore::new();
        assert!(store.is_empty());
        assert!(store.token_uri_csr.is_empty());
        assert!(store.image_uri_csr.is_empty());
        assert!(store.name_contract_csr.is_empty());
        assert!(store.name_nft_csr.is_empty());
        assert_eq!(store.rows_loaded, 0);
    }

    #[test]
    fn descending_evm_anchors_keep_largest_token_ids() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(2, &evm);
        for token in ["1", "10", "2"] {
            store
                .ingest_identity_row(IdentityRow {
                    chain: "ethereum".into(),
                    contract_address: "0xaaa".into(),
                    token_id: token.into(),
                    name_norm: "n".into(),
                    token_uri_norm: format!("uri://{token}"),
                    image_uri_norm: String::new(),
                    source_order: SourceOrder {
                        file_ordinal: 0,
                        file_row_number: token.parse().unwrap(),
                    },
                })
                .unwrap();
            store
                .ingest_metadata_anchor(
                    "ethereum",
                    "0xaaa",
                    token,
                    format!(r#"{{"name":"{token}"}}"#),
                    format!(r#"{{"name":"{token}"}}"#),
                    SourceOrder {
                        file_ordinal: 0,
                        file_row_number: token.parse().unwrap(),
                    },
                )
                .unwrap();
        }
        let tokens: Vec<_> = store.contracts[0]
            .metadata_by_token
            .iter()
            .map(|r| r.token_id.as_str())
            .collect();
        assert_eq!(tokens, ["10", "2"]);
    }

    #[test]
    fn contract_id_lookup_uses_interned_address() {
        let mut store = ResidentStore::new();
        store
            .ingest_identity_strs(
                "ethereum",
                "0xabc",
                "1",
                "Name",
                "ipfs://x",
                "",
                SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 0,
                },
            )
            .unwrap();
        assert_eq!(store.contract_id("ethereum", "0xabc"), Some(0));
        assert_eq!(store.contract_id("ethereum", "0xmissing"), None);
    }
}
