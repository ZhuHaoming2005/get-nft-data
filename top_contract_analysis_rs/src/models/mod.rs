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
