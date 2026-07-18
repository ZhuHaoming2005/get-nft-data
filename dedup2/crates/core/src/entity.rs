use ahash::AHashMap;
use serde::{Deserialize, Serialize};

pub type ContractId = u32;
pub type ChainId = u16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceOrder {
    pub file_ordinal: u32,
    pub file_row_number: u64,
}

#[derive(Clone, Debug)]
pub struct InputRow {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name_norm: String,
    pub token_uri_norm: String,
    pub image_uri_norm: String,
    pub metadata_json: String,
    pub source_order: SourceOrder,
}

#[derive(Clone, Debug)]
pub struct Contract {
    pub id: ContractId,
    pub chain_id: ChainId,
    pub address: String,
    pub name_norm: Option<String>,
    pub nft_count: u64,
    /// token_id -> first valid metadata json in source order
    pub metadata_by_token: AHashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct UriPosting {
    pub contract_id: ContractId,
    pub chain_id: ChainId,
    pub uri: String,
    /// token_ids that carry this URI (for NFT-level image AND-NOT)
    pub token_ids: Vec<String>,
}

impl UriPosting {
    pub fn nft_count(&self) -> u64 {
        self.token_ids.len() as u64
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChainTotals {
    pub contracts: u64,
    pub nfts: u64,
}

#[derive(Clone, Debug, Default)]
pub struct EntityStore {
    pub chains: Vec<String>,
    pub chain_ids: AHashMap<String, ChainId>,
    pub contracts: Vec<Contract>,
    pub contract_index: AHashMap<(ChainId, String), ContractId>,
    pub token_uri_postings: Vec<UriPosting>,
    pub image_uri_postings: Vec<UriPosting>,
    token_uri_index: AHashMap<(ContractId, String), usize>,
    image_uri_index: AHashMap<(ContractId, String), usize>,
    pub totals: AHashMap<ChainId, ChainTotals>,
    pub rows_loaded: u64,
}

impl EntityStore {
    pub fn chain_name(&self, id: ChainId) -> &str {
        &self.chains[id as usize]
    }

    pub fn ensure_chain(&mut self, chain: &str) -> ChainId {
        if let Some(id) = self.chain_ids.get(chain) {
            return *id;
        }
        let id = self.chains.len() as ChainId;
        self.chains.push(chain.to_owned());
        self.chain_ids.insert(chain.to_owned(), id);
        self.totals.insert(id, ChainTotals::default());
        id
    }

    pub fn ingest_row(&mut self, row: InputRow) {
        if row.chain.is_empty() || row.contract_address.is_empty() || row.token_id.is_empty() {
            return;
        }
        let chain_id = self.ensure_chain(&row.chain);
        let key = (chain_id, row.contract_address.clone());
        let contract_id = if let Some(id) = self.contract_index.get(&key).copied() {
            let contract = &mut self.contracts[id as usize];
            contract.nft_count += 1;
            if contract.name_norm.is_none() && !row.name_norm.is_empty() {
                contract.name_norm = Some(row.name_norm.clone());
            }
            if is_valid_metadata(&row.metadata_json) {
                contract
                    .metadata_by_token
                    .entry(row.token_id.clone())
                    .or_insert(row.metadata_json.clone());
            }
            id
        } else {
            let id = self.contracts.len() as ContractId;
            let mut metadata_by_token = AHashMap::new();
            if is_valid_metadata(&row.metadata_json) {
                metadata_by_token.insert(row.token_id.clone(), row.metadata_json.clone());
            }
            self.contracts.push(Contract {
                id,
                chain_id,
                address: row.contract_address.clone(),
                name_norm: (!row.name_norm.is_empty()).then_some(row.name_norm.clone()),
                nft_count: 1,
                metadata_by_token,
            });
            self.contract_index.insert(key, id);
            let totals = self.totals.entry(chain_id).or_default();
            totals.contracts += 1;
            id
        };

        let totals = self.totals.entry(chain_id).or_default();
        totals.nfts += 1;
        self.rows_loaded += 1;

        if !row.token_uri_norm.is_empty() {
            push_uri_posting(
                &mut self.token_uri_postings,
                &mut self.token_uri_index,
                contract_id,
                chain_id,
                &row.token_uri_norm,
                &row.token_id,
            );
        }
        if !row.image_uri_norm.is_empty() {
            push_uri_posting(
                &mut self.image_uri_postings,
                &mut self.image_uri_index,
                contract_id,
                chain_id,
                &row.image_uri_norm,
                &row.token_id,
            );
        }
    }

    /// Keep only listed chains (empty `allowed` means keep all). Reindexes contract ids.
    pub fn retain_chains(&mut self, allowed: &std::collections::BTreeSet<String>) {
        if allowed.is_empty() {
            return;
        }
        let keep_ids: ahash::AHashSet<ChainId> = self
            .chains
            .iter()
            .enumerate()
            .filter(|(_, name)| allowed.contains(*name))
            .map(|(idx, _)| idx as ChainId)
            .collect();
        if keep_ids.is_empty() || keep_ids.len() == self.chains.len() {
            return;
        }

        let old_contracts = std::mem::take(&mut self.contracts);
        let old_token = std::mem::take(&mut self.token_uri_postings);
        let old_image = std::mem::take(&mut self.image_uri_postings);
        self.contract_index.clear();
        self.token_uri_index.clear();
        self.image_uri_index.clear();
        self.totals.clear();
        self.rows_loaded = 0;

        let mut id_map: AHashMap<ContractId, ContractId> = AHashMap::new();
        for contract in old_contracts {
            if !keep_ids.contains(&contract.chain_id) {
                continue;
            }
            let new_id = self.contracts.len() as ContractId;
            id_map.insert(contract.id, new_id);
            self.contract_index
                .insert((contract.chain_id, contract.address.clone()), new_id);
            let totals = self.totals.entry(contract.chain_id).or_default();
            totals.contracts += 1;
            totals.nfts += contract.nft_count;
            self.rows_loaded += contract.nft_count;
            self.contracts.push(Contract {
                id: new_id,
                ..contract
            });
        }

        for posting in old_token {
            let Some(&new_id) = id_map.get(&posting.contract_id) else {
                continue;
            };
            for token_id in &posting.token_ids {
                push_uri_posting(
                    &mut self.token_uri_postings,
                    &mut self.token_uri_index,
                    new_id,
                    posting.chain_id,
                    &posting.uri,
                    token_id,
                );
            }
        }
        for posting in old_image {
            let Some(&new_id) = id_map.get(&posting.contract_id) else {
                continue;
            };
            for token_id in &posting.token_ids {
                push_uri_posting(
                    &mut self.image_uri_postings,
                    &mut self.image_uri_index,
                    new_id,
                    posting.chain_id,
                    &posting.uri,
                    token_id,
                );
            }
        }
    }
}

fn push_uri_posting(
    postings: &mut Vec<UriPosting>,
    index: &mut AHashMap<(ContractId, String), usize>,
    contract_id: ContractId,
    chain_id: ChainId,
    uri: &str,
    token_id: &str,
) {
    let key = (contract_id, uri.to_owned());
    if let Some(&pos) = index.get(&key) {
        postings[pos].token_ids.push(token_id.to_owned());
        return;
    }
    let pos = postings.len();
    postings.push(UriPosting {
        contract_id,
        chain_id,
        uri: uri.to_owned(),
        token_ids: vec![token_id.to_owned()],
    });
    index.insert(key, pos);
}

pub fn is_valid_metadata(content: &str) -> bool {
    let trimmed = content.trim();
    if trimmed.is_empty() || trimmed.len() > 64 * 1024 {
        return false;
    }
    let starts_ok = trimmed.starts_with('{') || trimmed.starts_with('[');
    if !starts_ok {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(trimmed).is_ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    Name,
    TokenUri,
    ImageUri,
    Metadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeKind {
    IntraChain,
    CrossChainSummary,
    ChainMatrix,
}
