use std::collections::{BTreeMap, HashSet};

use crate::models::{
    EthTransferRecord, NftSaleRecord, SeedNft, TransactionReceiptRecord, TransferRecord,
};

mod collection;
mod history;
mod transaction;

pub use collection::{fetch_helius_collection_assets, fetch_helius_collection_snapshot};
pub use history::{
    fetch_helius_asset_transfers, fetch_helius_assets_history,
    fetch_helius_assets_history_with_budget, fetch_helius_assets_transfers,
    fetch_helius_collection_transfers,
};
pub use transaction::fetch_helius_transaction_details;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeliusCollectionAsset {
    pub nft: SeedNft,
    pub owner_address: String,
    pub compressed: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeliusCollectionSnapshot {
    pub assets: Vec<HeliusCollectionAsset>,
    pub total: usize,
    pub collection_name: String,
    pub collection_symbol: String,
    pub collection_authority: String,
    pub truncated: bool,
    pub coverage_ratio: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeliusCollectionHistory {
    pub transfers: Vec<TransferRecord>,
    pub sales: Vec<NftSaleRecord>,
    pub failed_asset_count: usize,
    pub requested_asset_count: usize,
    pub successful_asset_count: usize,
    pub complete_asset_count: usize,
    pub unrequested_asset_count: usize,
    pub truncated_asset_history_count: usize,
    pub fetched_transaction_count: usize,
    /// Number of unique transaction payloads decoded into common receipt/payment details.
    pub parsed_transaction_count: usize,
    pub reported_transaction_count: usize,
    pub failed_transaction_count: usize,
    pub signature_discovery_failure_count: usize,
    pub transaction_detail_failure_count: usize,
    pub unattributed_native_transaction_count: usize,
    pub unattributed_native_transaction_signatures: HashSet<String>,
    pub unresolved_compressed_mint_count: usize,
    pub complete: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeliusTransactionDetails {
    pub receipt: TransactionReceiptRecord,
    pub native_transfers: Vec<EthTransferRecord>,
    pub pre_balances_native: BTreeMap<String, f64>,
    pub account_keys: Vec<String>,
    pub native_transfer_attribution_complete: bool,
}
