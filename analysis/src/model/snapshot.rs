use crate::model::{
    ChainId, ContractId, MetadataId, NameValueId, NftId, SourceOrder, TokenIdId, UriValueId,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct InputRow {
    pub chain: ChainId,
    pub contract_address: String,
    pub token_id: String,
    pub name_norm: Option<String>,
    pub token_uri_norm: Option<String>,
    pub image_uri_norm: Option<String>,
    pub metadata_json: Option<String>,
    pub source_order: SourceOrder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NftIdentityRecord {
    pub contract_id: ContractId,
    pub token_id_id: TokenIdId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UriFeatureRecord {
    pub token_uri: Option<UriValueId>,
    pub image_uri: Option<UriValueId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContractRecord {
    pub chain: ChainId,
    pub address: Arc<str>,
    pub nft_count: u64,
    pub name_value_id: Option<NameValueId>,
    pub metadata_profile_id: Option<crate::model::ProfileId>,
    pub name_owner_shard: Option<u8>,
    pub metadata_owner_shard: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataAnchor {
    pub token_id_id: TokenIdId,
    pub metadata_id: MetadataId,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InputQuality {
    pub physical_rows: u64,
    pub logical_nfts: u64,
    pub duplicate_rows: u64,
    pub conflicting_rows: u64,
    pub empty_names: u64,
    pub empty_token_uris: u64,
    pub empty_image_uris: u64,
    pub invalid_metadata: u64,
    pub oversized_metadata: u64,
    pub non_anchor_metadata: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalNftRef {
    pub nft_id: NftId,
    pub contract_id: ContractId,
}
