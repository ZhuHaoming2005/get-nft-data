use dedup_model::{ChainId, Dimension};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatisticsRow {
    pub dimension: Dimension,
    pub subtype: String,
    pub scope: String,
    pub primary_chain: ChainId,
    pub secondary_chain: Option<ChainId>,
    pub total_contracts: u64,
    pub total_nfts: u64,
    pub duplicate_contract_count: u64,
    pub duplicate_nft_count: u64,
    pub is_approximate: bool,
    pub run_status: String,
}
