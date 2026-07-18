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
    pub nft_count: u64,
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
                contract_id,
                chain_id,
                &row.token_uri_norm,
            );
        }
        if !row.image_uri_norm.is_empty() {
            push_uri_posting(
                &mut self.image_uri_postings,
                contract_id,
                chain_id,
                &row.image_uri_norm,
            );
        }
    }
}

fn push_uri_posting(
    postings: &mut Vec<UriPosting>,
    contract_id: ContractId,
    chain_id: ChainId,
    uri: &str,
) {
    if let Some(last) = postings.last_mut()
        && last.contract_id == contract_id
        && last.uri == uri
    {
        last.nft_count += 1;
        return;
    }
    postings.push(UriPosting {
        contract_id,
        chain_id,
        uri: uri.to_owned(),
        nft_count: 1,
    });
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
