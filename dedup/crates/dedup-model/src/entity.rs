use crate::{ChainId, ContractId, NftId, SourceOrder, StringId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Contract {
    pub id: ContractId,
    pub chain_id: ChainId,
    pub address_ref: StringId,
    pub name_ref: Option<StringId>,
    pub first_nft_id: NftId,
    pub nft_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Nft {
    pub id: NftId,
    pub contract_id: ContractId,
    pub token_id_ref: StringId,
    pub token_uri_ref: Option<StringId>,
    pub image_uri_ref: Option<StringId>,
    pub has_metadata: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRow {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name_norm: String,
    pub token_uri_norm: String,
    pub image_uri_norm: String,
    pub metadata_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_valid: Option<bool>,
    pub source_order: SourceOrder,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityArtifacts {
    pub contracts: Vec<Contract>,
    pub nfts: Vec<Nft>,
}

pub trait MetadataSourceValidator: Send + Sync {
    fn is_valid_metadata(&self, content: &str) -> bool;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistedEntityArtifacts {
    pub entities: EntityArtifacts,
    pub strings: Vec<Vec<u8>>,
    pub metadata_by_nft: Vec<(NftId, String)>,
}
