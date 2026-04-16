use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedNft {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub token_uri: String,
    pub image_uri: String,
    pub metadata_json: String,
    pub metadata_doc: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseNftRecord {
    pub contract_address: String,
    pub token_id: String,
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub symbol: String,
    pub metadata_json: String,
    pub metadata_doc: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateCandidate {
    pub contract_address: String,
    pub token_id: String,
    pub match_reasons: Vec<String>,
    pub confidence: String,
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferRecord {
    pub contract_address: String,
    pub token_id: String,
    pub tx_hash: String,
    pub log_index: i64,
    pub block_number: i64,
    pub block_time: i64,
    pub from_address: String,
    pub to_address: String,
    pub event_type: String,
    pub source: String,
}

impl TransferRecord {
    pub fn mint(
        contract_address: impl Into<String>,
        token_id: impl Into<String>,
        block_time: i64,
        to_address: impl Into<String>,
    ) -> Self {
        Self {
            contract_address: contract_address.into(),
            token_id: token_id.into(),
            block_time,
            from_address: ZERO_ADDRESS.to_string(),
            to_address: to_address.into(),
            event_type: "mint".into(),
            source: "test".into(),
            ..Self::default()
        }
    }

    pub fn transfer(
        contract_address: impl Into<String>,
        token_id: impl Into<String>,
        block_time: i64,
        from_address: impl Into<String>,
        to_address: impl Into<String>,
    ) -> Self {
        Self {
            contract_address: contract_address.into(),
            token_id: token_id.into(),
            block_time,
            from_address: from_address.into(),
            to_address: to_address.into(),
            event_type: "transfer".into(),
            source: "test".into(),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AddressSignals {
    pub mint_address_count: usize,
    pub mint_count: usize,
    pub unique_receiver_count: usize,
    pub cycle_edge_count: usize,
    pub star_distributor_count: usize,
    pub mint_to_first_transfer_seconds: i64,
    pub fast_spread: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct InfringingTokenRecord {
    pub contract_address: String,
    pub token_id: String,
    pub mint_tx_hash: String,
    pub mint_block: i64,
    pub minter_address: String,
    pub first_transfer_time: i64,
    pub history_window: String,
    pub match_reasons: Vec<String>,
    pub candidate_open_license: bool,
    pub official_or_legit_reissue: bool,
}

pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
