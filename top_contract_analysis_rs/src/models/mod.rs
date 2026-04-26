use std::collections::BTreeMap;

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
    #[serde(skip_serializing, skip_deserializing, default)]
    pub metadata_recall_checked: bool,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub metadata_recall_match: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractNameRecord {
    pub contract_address: String,
    pub name_norm: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractSignal {
    pub contract_address: String,
    pub token_count: usize,
    pub uri_match_count: usize,
    pub image_match_count: usize,
    pub name_prefix_match: bool,
    pub keyword_match: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerBalance {
    pub owner_address: String,
    pub token_balances: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseSnapshot {
    pub nft_rows: Vec<DatabaseNftRecord>,
    pub contract_names: Vec<ContractNameRecord>,
    pub contract_signals: BTreeMap<String, ContractSignal>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateCandidate {
    pub contract_address: String,
    pub token_id: String,
    pub match_reasons: Vec<String>,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub confidence: String,
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractMetadata {
    pub chain: String,
    pub contract_address: String,
    pub token_type: String,
    pub contract_deployer: String,
    pub deployed_block_number: i64,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct NftSaleRecord {
    pub contract_address: String,
    pub token_id: String,
    pub tx_hash: String,
    pub block_number: i64,
    pub log_index: i64,
    pub bundle_index: i64,
    pub buyer_address: String,
    pub seller_address: String,
    pub marketplace: String,
    pub taker: String,
    pub payment_token_symbol: String,
    pub payment_token_address: String,
    pub price_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_usd: Option<f64>,
    pub seller_fee_eth: f64,
    #[serde(default)]
    pub seller_fee_usd: f64,
    pub protocol_fee_eth: f64,
    #[serde(default)]
    pub protocol_fee_usd: f64,
    pub royalty_fee_eth: f64,
    #[serde(default)]
    pub royalty_fee_usd: f64,
    pub source: String,
    pub is_native_eth: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionReceiptRecord {
    pub tx_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
    pub from_address: String,
    pub gas_used: i64,
    pub effective_gas_price_wei: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct EthTransferRecord {
    pub tx_hash: String,
    pub block_number: i64,
    pub from_address: String,
    pub to_address: String,
    pub value_eth: f64,
    pub category: String,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedContractPayload {
    pub chain: String,
    pub contract_address: String,
    pub name: String,
    pub symbol: String,
    pub token_type: String,
    pub contract_deployer: String,
    pub deployed_block_number: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ReportSummary {
    pub open_license_detected: bool,
    pub candidate_contract_count: i64,
    pub infringing_nft_count: i64,
    pub malicious_address_count: i64,
    pub honest_address_count: i64,
    pub repeat_infringing_address_count: i64,
    pub legit_duplicate_contract_count: i64,
    pub candidate_open_license_token_count: i64,
    pub candidate_open_license_contract_count: i64,
    pub honest_purchase_total_eth: f64,
    #[serde(default)]
    pub honest_purchase_total_usd: f64,
    pub stuck_cost_eth: f64,
    #[serde(default)]
    pub stuck_cost_usd: f64,
    pub stuck_cost_ratio: Option<f64>,
    pub buy_asset_ratio_known_address_count: i64,
    pub ratio_over_60_address_count: i64,
    pub ratio_over_60_address_ratio: Option<f64>,
    pub ratio_over_80_address_count: i64,
    pub ratio_over_80_address_ratio: Option<f64>,
    pub stuck_honest_address_count: i64,
    pub stuck_honest_address_ratio: Option<f64>,
    pub corrupted_honest_address_count: i64,
    pub avg_seconds_to_honest_holder: Option<f64>,
    pub median_seconds_to_honest_holder: Option<f64>,
    pub avg_mint_to_first_transfer_seconds: Option<f64>,
    pub median_mint_to_first_transfer_seconds: Option<f64>,
    pub avg_unique_receiver_count: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedCollectionStatsPayload {
    pub seed_nft_count: i64,
    pub unique_token_uri_count: i64,
    pub unique_image_uri_count: i64,
    pub unique_name_count: i64,
    pub unique_symbol_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateContractPayload {
    pub contract_address: String,
    pub candidate_count: i64,
    pub match_reasons: Vec<String>,
    pub mint_recipients: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddressSignalPayload {
    pub mint_address_count: i64,
    pub mint_count: i64,
    pub unique_receiver_count: i64,
    pub cycle_edge_count: i64,
    pub star_distributor_count: i64,
    pub mint_to_first_transfer_seconds: i64,
    pub fast_spread: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct VictimSignalPayload {
    pub owner_count: i64,
    pub stuck_holder_count: i64,
    pub stuck_holder_ratio: Option<f64>,
    pub victim_wallet_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct HonestAddressStatsPayload {
    pub honest_address_count: i64,
    pub corrupted_address_count: i64,
    pub honest_to_honest_transfer_count: i64,
    pub median_holding_seconds: Option<f64>,
    pub avg_seconds_to_honest_holder: Option<f64>,
    #[serde(default)]
    pub corrupted_addresses: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct HonestAddressPayload {
    pub contract_address: String,
    pub address: String,
    pub interacted_token_count: i64,
    pub currently_holding_token_count: i64,
    pub hold_duration_median_seconds: Option<f64>,
    pub hold_duration_count: i64,
    pub is_corrupted_address: bool,
    pub honest_sale_to_honest_count: i64,
    pub mint_to_honest_seconds_samples: Vec<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct VictimAddressPayload {
    pub address: String,
    pub buy_tx_hashes: Vec<String>,
    pub buy_amount_eth: f64,
    #[serde(default)]
    pub buy_amount_usd: f64,
    pub last_buy_amount_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_buy_amount_usd: Option<f64>,
    pub buy_before_eth_balance: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_before_usd_balance: Option<f64>,
    pub buy_asset_ratio: Option<f64>,
    pub buy_asset_ratio_with_gas: Option<f64>,
    pub is_stuck: bool,
    pub last_buy_tx_hash: String,
    #[serde(default = "default_ratio_status")]
    pub ratio_status: String,
}

fn default_ratio_status() -> String {
    "unavailable".into()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct FraudTradeStatsPayload {
    pub unique_buyers: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd_priced_sale_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd_priced_volume: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eth_priced_sale_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eth_priced_volume: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_eth_sale_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_eth_volume: Option<f64>,
    pub stuck_wallet_count: i64,
    pub stuck_cost_eth: f64,
    #[serde(default)]
    pub stuck_cost_usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputFilesPayload {
    pub json: String,
    pub markdown: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractLevelSummaryPayload {
    pub candidate_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaliciousAddressPayload {
    pub address: String,
    pub mint_role: bool,
    pub wash_cycle_count: i64,
    pub star_out_degree: i64,
    pub rapid_spread_contracts: Vec<String>,
    pub evidence_contracts: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SingleReportPayload {
    pub seed_contract: SeedContractPayload,
    pub seed_collection_stats: SeedCollectionStatsPayload,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub duplicate_candidates: Vec<DuplicateCandidate>,
    pub contract_level_summary: BTreeMap<String, ContractLevelSummaryPayload>,
    pub report_summary: ReportSummary,
    pub duplicate_contracts: Vec<DuplicateContractPayload>,
    pub legit_duplicates: Vec<DuplicateContractPayload>,
    pub address_signals: BTreeMap<String, AddressSignalPayload>,
    pub victim_signals: BTreeMap<String, VictimSignalPayload>,
    pub infringing_tokens: Vec<InfringingTokenRecord>,
    pub malicious_addresses: Vec<MaliciousAddressPayload>,
    pub honest_addresses: Vec<HonestAddressPayload>,
    pub honest_address_stats: BTreeMap<String, HonestAddressStatsPayload>,
    pub victim_addresses: Vec<VictimAddressPayload>,
    pub fraud_trade_stats: BTreeMap<String, FraudTradeStatsPayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BatchReportSummary {
    pub seed_report_count: i64,
    pub chain: String,
    pub chains: Vec<String>,
    pub open_license_detected_count: i64,
    pub candidate_contract_count_total: i64,
    pub infringing_nft_count_total: i64,
    pub malicious_address_count_total: i64,
    pub honest_address_count_total: i64,
    pub repeat_infringing_address_count_total: i64,
    pub repeat_infringing_address_count_global: i64,
    pub legit_duplicate_contract_count_total: i64,
    pub honest_purchase_total_eth_total: f64,
    #[serde(default)]
    pub honest_purchase_total_usd_total: f64,
    pub stuck_cost_eth_total: f64,
    #[serde(default)]
    pub stuck_cost_usd_total: f64,
    pub stuck_cost_ratio_overall: Option<f64>,
    pub buy_asset_ratio_known_address_count_total: i64,
    pub ratio_over_60_address_count_total: i64,
    pub ratio_over_60_address_ratio_overall: Option<f64>,
    pub ratio_over_80_address_count_total: i64,
    pub ratio_over_80_address_ratio_overall: Option<f64>,
    pub stuck_honest_address_count_total: i64,
    pub stuck_honest_address_ratio_overall: Option<f64>,
    pub corrupted_honest_address_count_total: i64,
    pub avg_seconds_to_honest_holder_mean: Option<f64>,
    pub median_seconds_to_honest_holder_median: Option<f64>,
    pub avg_mint_to_first_transfer_seconds_mean: Option<f64>,
    pub median_mint_to_first_transfer_seconds_median: Option<f64>,
    pub avg_unique_receiver_count_mean: Option<f64>,
    pub generated_at: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BatchSeedReportPayload {
    pub seed_contract: SeedContractPayload,
    pub report_summary: ReportSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_files: Option<OutputFilesPayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BatchSummaryPayload {
    pub batch_summary: BatchReportSummary,
    pub seed_reports: Vec<BatchSeedReportPayload>,
}
