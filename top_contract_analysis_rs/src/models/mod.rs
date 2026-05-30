use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, Serializer};

fn default_aggregate_count() -> i64 {
    1
}

fn is_one_i64(value: &i64) -> bool {
    *value == 1
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_true() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

fn single_seed_report_type() -> String {
    "single_seed".into()
}

fn batch_summary_report_type() -> String {
    "batch_summary".into()
}

fn serialize_single_seed_report_type<S>(_value: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("single_seed")
}

fn serialize_batch_summary_report_type<S>(_value: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("batch_summary")
}

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
pub struct ContractDuplicateRecord {
    pub contract_address: String,
    pub representative: DatabaseNftRecord,
    pub token_uri_match: bool,
    pub image_uri_match: bool,
    pub name_norms: Vec<String>,
    pub metadata_token_rows: Vec<DatabaseNftRecord>,
    pub metadata_recall_checked: bool,
    pub metadata_recall_match: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerBalance {
    pub owner_address: String,
    pub token_balances: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseSnapshot {
    pub nft_rows: Vec<DatabaseNftRecord>,
    pub duplicate_contract_rows: Vec<ContractDuplicateRecord>,
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
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub deployed_block_time: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub admin_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub proxy_admin_address: String,
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
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub royalty_recipient_address: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payment_token_symbol: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payment_token_address: String,
    pub category: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct NftMarketEventRecord {
    pub contract_address: String,
    pub token_id: String,
    pub event_type: String,
    pub order_type: String,
    pub tx_hash: String,
    pub order_hash: String,
    pub block_number: i64,
    pub block_time: i64,
    pub event_timestamp: i64,
    pub actor_address: String,
    pub from_address: String,
    pub to_address: String,
    pub maker_address: String,
    pub taker_address: String,
    pub marketplace: String,
    pub payment_token_symbol: String,
    pub payment_token_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_usd: Option<f64>,
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
    pub first_transfer_delay_seconds: i64,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclusion_reasons: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub contract_deployer: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub deployed_block_number: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub deployed_block_time: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub admin_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub proxy_admin_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub symbol: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddressSignalPayload {
    pub mint_address_count: i64,
    pub mint_count: i64,
    pub unique_receiver_count: i64,
    pub cycle_edge_count: i64,
    pub star_distributor_count: i64,
    pub first_transfer_delay_seconds: i64,
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
pub struct HonestAddressPayload {
    pub contract_address: String,
    pub address: String,
    pub interacted_token_count: i64,
    pub currently_holding_token_count: i64,
    pub hold_duration_median_seconds: Option<f64>,
    pub hold_duration_count: i64,
    pub is_corrupted_address: bool,
    pub victim_resale_count: i64,
    pub deployment_to_neutral_holder_seconds_samples: Vec<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SecondarySaleVictimAddressPayload {
    pub contract_address: String,
    pub address: String,
    pub buy_tx_hashes: Vec<String>,
    pub buy_amount_eth: f64,
    #[serde(default)]
    pub buy_amount_usd: f64,
    pub last_buy_amount_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_buy_amount_usd: Option<f64>,
    pub is_stuck: bool,
    pub last_buy_tx_hash: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct VictimAcquisitionAddressPayload {
    pub address: String,
    pub contract_addresses: Vec<String>,
    pub acquisition_channels: Vec<String>,
    pub attribution_labels: Vec<String>,
    pub tx_hashes: Vec<String>,
    pub secondary_sale_cost_eth: f64,
    pub secondary_sale_cost_usd: f64,
    pub secondary_sale_stuck_cost_eth: f64,
    pub secondary_sale_stuck_cost_usd: f64,
    pub secondary_sale_count: i64,
    pub paid_mint_cost_eth: f64,
    pub paid_mint_cost_usd: f64,
    pub paid_mint_stuck_cost_eth: f64,
    pub paid_mint_stuck_cost_usd: f64,
    pub paid_mint_edge_count: i64,
    pub paid_mint_stuck_token_count: i64,
    pub total_acquisition_cost_eth: f64,
    pub total_acquisition_cost_usd: f64,
    pub total_stuck_cost_eth: f64,
    pub total_stuck_cost_usd: f64,
    pub is_stuck: bool,
    pub is_corrupted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_before_eth_balance: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_before_usd_balance: Option<f64>,
    pub buy_asset_ratio: Option<f64>,
    pub buy_asset_ratio_with_gas: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AddressEvidencePayload {
    pub evidence_type: String,
    pub contract_address: String,
    pub token_id: String,
    pub tx_hash: String,
    pub weight: f64,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AddressAttributionPayload {
    pub contract_address: String,
    pub address: String,
    pub observed_roles: Vec<String>,
    #[serde(default)]
    pub attribution_label: String,
    #[serde(default)]
    pub operator_score: f64,
    #[serde(default)]
    pub colluder_score: f64,
    pub attacker_score: f64,
    pub victim_score: f64,
    #[serde(default)]
    pub corruption_score: f64,
    #[serde(default)]
    pub neutral_score: f64,
    pub confidence: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub operator_level: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operator_level_label: String,
    pub evidence: Vec<AddressEvidencePayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AddressEvidenceFeaturePayload {
    pub contract_address: String,
    pub address: String,
    pub observed_roles: Vec<String>,
    pub attribution_label: String,
    pub operator_score: f64,
    pub colluder_score: f64,
    pub victim_score: f64,
    pub corruption_score: f64,
    pub neutral_score: f64,
    pub confidence: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub operator_level: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operator_level_label: String,
    pub evidence_count: i64,
    pub related_token_count: i64,
    pub related_tx_count: i64,
    pub evidence: Vec<AddressEvidencePayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ContractLifecycleEventPayload {
    pub event_id: String,
    pub contract_address: String,
    pub lifecycle_stage: String,
    pub event_type: String,
    pub block_number: i64,
    pub block_time: i64,
    pub tx_hash: String,
    pub actor_address: String,
    pub counterparty_address: String,
    pub token_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_usd: Option<f64>,
    pub evidence_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_flags: Vec<String>,
    pub confidence: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ValueFlowEdgePayload {
    pub edge_id: String,
    pub contract_address: String,
    pub from_address: String,
    pub to_address: String,
    pub tx_hash: String,
    pub block_number: i64,
    pub block_time: i64,
    pub token_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_with_gas_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_with_gas_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_before_eth_balance: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_before_usd_balance: Option<f64>,
    pub payment_token_symbol: String,
    pub payment_token_address: String,
    pub channel: String,
    pub marketplace: String,
    pub evidence_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from_role: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to_role: String,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub recipient_known: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_flags: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentSimilarityEdgePayload {
    pub edge_id: String,
    pub seed_contract_address: String,
    pub candidate_contract_address: String,
    pub token_id: String,
    pub match_reasons: Vec<String>,
    pub confidence: String,
    pub evidence_type: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CampaignClusterPayload {
    pub cluster_id: String,
    pub seed_contract_address: String,
    pub contract_addresses: Vec<String>,
    pub suspected_operator_addresses: Vec<String>,
    pub suspected_colluder_addresses: Vec<String>,
    pub victim_addresses: Vec<String>,
    pub corrupted_victim_addresses: Vec<String>,
    pub lifecycle_stages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shared_evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub value_flow_channels: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cluster_confidence: String,
    pub token_count: i64,
    pub sale_count: i64,
    pub value_flow_count: i64,
    pub gross_revenue_eth: f64,
    pub gross_revenue_usd: f64,
    #[serde(default)]
    pub operator_revenue_eth: f64,
    #[serde(default)]
    pub operator_revenue_usd: f64,
    #[serde(default)]
    pub marketplace_fee_eth: f64,
    #[serde(default)]
    pub marketplace_fee_usd: f64,
    #[serde(default)]
    pub funding_amount_eth: f64,
    #[serde(default)]
    pub funding_amount_usd: f64,
    #[serde(default)]
    pub withdrawal_amount_eth: f64,
    #[serde(default)]
    pub withdrawal_amount_usd: f64,
    #[serde(default)]
    pub funding_edge_count: i64,
    #[serde(default)]
    pub withdrawal_edge_count: i64,
    #[serde(default)]
    pub revenue_backflow_edge_count: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value_flow_coverage_scope: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub value_flow_coverage_gaps: Vec<String>,
    pub first_block_number: i64,
    pub last_block_number: i64,
    pub first_block_time: i64,
    pub last_block_time: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ContractLifecycleMetricPayload {
    pub contract_address: String,
    #[serde(default)]
    pub deployment_time: i64,
    pub first_mint_time: i64,
    #[serde(default)]
    pub first_transfer_time: i64,
    pub first_listing_time: i64,
    pub first_sale_time: i64,
    pub first_victim_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_first_transfer_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_first_listing_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_first_sale_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_first_victim_seconds: Option<i64>,
    pub cascade_node_count: i64,
    pub cascade_edge_count: i64,
    pub victim_count: i64,
    pub sale_count: i64,
    pub market_event_count: i64,
    pub gross_revenue_eth: f64,
    pub gross_revenue_usd: f64,
    #[serde(default)]
    pub operator_revenue_eth: f64,
    #[serde(default)]
    pub operator_revenue_usd: f64,
    #[serde(default)]
    pub marketplace_fee_eth: f64,
    #[serde(default)]
    pub marketplace_fee_usd: f64,
    #[serde(default)]
    pub funding_amount_eth: f64,
    #[serde(default)]
    pub funding_amount_usd: f64,
    #[serde(default)]
    pub withdrawal_amount_eth: f64,
    #[serde(default)]
    pub withdrawal_amount_usd: f64,
    #[serde(default)]
    pub funding_edge_count: i64,
    #[serde(default)]
    pub withdrawal_edge_count: i64,
    #[serde(default)]
    pub revenue_backflow_edge_count: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value_flow_coverage_scope: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub value_flow_coverage_gaps: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub top_value_recipient_address: String,
    #[serde(default)]
    pub top_value_recipient_eth: f64,
    #[serde(default)]
    pub top_value_recipient_usd: f64,
    #[serde(default)]
    pub top_value_recipient_share: Option<f64>,
    #[serde(default)]
    pub pre_sale_signal_count: i64,
    #[serde(default)]
    pub early_detection_positive: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WeakSupervisionLabelPayload {
    pub entity_type: String,
    pub contract_address: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub address: String,
    pub label: String,
    pub confidence: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_flags: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EarlyDetectionFeaturePayload {
    pub contract_address: String,
    pub observation_window_seconds: i64,
    pub window_start_time: i64,
    pub window_end_time: i64,
    pub content_similarity_count: i64,
    pub mint_event_count: i64,
    pub market_event_count: i64,
    pub value_flow_count: i64,
    pub funding_edge_count: i64,
    pub withdrawal_edge_count: i64,
    pub sale_event_count: i64,
    pub victim_signal_count: i64,
    pub pre_sale_signal_count: i64,
    pub weak_label: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct OperatorLevelStatsPayload {
    pub level: i64,
    pub level_label: String,
    pub address_count: i64,
    pub secondary_sale_cost_eth: f64,
    pub secondary_sale_cost_usd: f64,
    pub paid_mint_cost_eth: f64,
    pub paid_mint_cost_usd: f64,
    pub acquisition_total_eth: f64,
    pub acquisition_total_usd: f64,
    pub acquisition_edge_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperDuplicateScaleRowPayload {
    pub category: String,
    pub duplicate_nft_count: i64,
    pub duplicate_nft_ratio: Option<f64>,
    pub duplicate_nft_ratio_numerator: i64,
    pub duplicate_nft_ratio_denominator: i64,
    pub duplicate_contract_count: i64,
    pub duplicate_contract_ratio: Option<f64>,
    pub duplicate_contract_ratio_numerator: i64,
    pub duplicate_contract_ratio_denominator: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperAddressClassificationPayload {
    pub malicious_address_count: i64,
    pub repeat_infringing_malicious_address_count: i64,
    pub honest_address_count: i64,
    pub total_address_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperWashTradingRowPayload {
    pub cycle_id: String,
    pub participant_node_count: i64,
    pub token_gini: Option<f64>,
    pub avg_cycle_blocks: Option<f64>,
    pub fake_volume_eth: f64,
    pub fake_volume_usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperPumpExitRowPayload {
    pub cycle_id: String,
    pub exit_delay_seconds: Option<i64>,
    pub exit_price_premium: Option<f64>,
    pub exit_ratio: Option<f64>,
    pub exit_ratio_numerator: i64,
    pub exit_ratio_denominator: i64,
    pub linked_honest_buyer_count: i64,
    pub linked_loss_eth: f64,
    pub linked_loss_usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperStarBehaviorRowPayload {
    pub behavior: String,
    pub centers: i64,
    pub edges: i64,
    pub wallets: i64,
    pub tokens: i64,
    pub avg_fan_out: Option<f64>,
    pub avg_fan_out_numerator: i64,
    pub avg_fan_out_denominator: i64,
    pub median_holding_seconds: Option<f64>,
    pub total_value_eth: f64,
    pub total_value_usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperLayeredTransferRowPayload {
    pub path_id: String,
    pub tokens: i64,
    pub length: i64,
    pub wallets: i64,
    pub zero_or_low_value_hops: i64,
    pub total_path_duration_seconds: Option<i64>,
    pub total_value_eth: f64,
    pub total_value_usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperInventoryConcentrationRowPayload {
    pub hub_address: String,
    pub source_wallets: i64,
    pub inbound_txns: i64,
    pub token_share: Option<f64>,
    pub token_share_numerator: i64,
    pub token_share_denominator: i64,
    pub value_collected_eth: f64,
    pub value_collected_usd: f64,
    pub value_share: Option<f64>,
    pub value_share_numerator: f64,
    pub value_share_denominator: f64,
    pub collection_window_seconds: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperHonestBuyerRowPayload {
    pub honest_buyer: String,
    pub fake_nft_bought: i64,
    pub total_paid_eth: f64,
    pub total_paid_usd: f64,
    pub source_pattern: String,
    pub time_to_purchase_seconds: Option<i64>,
    pub still_holding: bool,
    pub holding_seconds: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperContractBehaviorStatsPayload {
    pub contract_address: String,
    pub wash_trading: Vec<PaperWashTradingRowPayload>,
    pub pump_and_exit: Vec<PaperPumpExitRowPayload>,
    pub star_behaviors: Vec<PaperStarBehaviorRowPayload>,
    pub layered_transfers: Vec<PaperLayeredTransferRowPayload>,
    pub inventory_concentration: Vec<PaperInventoryConcentrationRowPayload>,
    pub honest_buyers_top: Vec<PaperHonestBuyerRowPayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperBehaviorSummaryRowPayload {
    pub behavior_type: String,
    pub contract_count: i64,
    pub contract_coverage_ratio: Option<f64>,
    pub contract_coverage_numerator: i64,
    pub contract_coverage_denominator: i64,
    pub instance_count: i64,
    pub instance_ratio: Option<f64>,
    pub instance_ratio_numerator: i64,
    pub instance_ratio_denominator: i64,
    pub address_count: i64,
    pub nft_count: i64,
    pub linked_buyer_count: i64,
    pub linked_loss_eth: f64,
    pub linked_loss_usd: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperAttackerCostPayload {
    pub setup_gas_eth: f64,
    pub setup_gas_usd: f64,
    pub lure_gas_eth: f64,
    pub lure_gas_usd: f64,
    pub exit_gas_eth: f64,
    pub exit_gas_usd: f64,
    pub total_gas_eth: f64,
    pub total_gas_usd: f64,
    pub top_contract_contribution_ratio: Option<f64>,
    pub top_contract_contribution_numerator: f64,
    pub top_contract_contribution_denominator: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperHonestLossPayload {
    pub category: String,
    pub stuck_nft_count: i64,
    pub stuck_nft_ratio: Option<f64>,
    pub stuck_nft_ratio_numerator: i64,
    pub stuck_nft_ratio_denominator: i64,
    pub stuck_time_ratio: Option<f64>,
    pub stuck_time_ratio_numerator: f64,
    pub stuck_time_ratio_denominator: f64,
    pub secondary_sale_loss_eth: f64,
    pub secondary_sale_loss_usd: f64,
    pub paid_mint_loss_eth: f64,
    pub paid_mint_loss_usd: f64,
    pub total_loss_eth: f64,
    pub total_loss_usd: f64,
    pub top_contract_loss_contribution_ratio: Option<f64>,
    pub top_contract_loss_contribution_numerator: f64,
    pub top_contract_loss_contribution_denominator: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperDataQualityPayload {
    #[serde(default)]
    pub representative_candidate_count: i64,
    #[serde(default)]
    pub candidate_contract_count: i64,
    #[serde(default)]
    pub suspected_duplicate_contract_count: i64,
    #[serde(default)]
    pub infringing_nft_count: i64,
    pub sale_price_parseable_count: i64,
    pub sale_price_total_count: i64,
    pub sale_price_parseable_ratio: Option<f64>,
    pub legit_duplicate_contract_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PaperStatsPayload {
    pub duplicate_scale: Vec<PaperDuplicateScaleRowPayload>,
    pub address_classification: PaperAddressClassificationPayload,
    pub contract_behavior_stats: Vec<PaperContractBehaviorStatsPayload>,
    pub malicious_behavior_summary: Vec<PaperBehaviorSummaryRowPayload>,
    pub attacker_cost: PaperAttackerCostPayload,
    pub honest_loss: Vec<PaperHonestLossPayload>,
    pub data_quality: PaperDataQualityPayload,
    #[serde(default, skip_serializing)]
    pub malicious_addresses: Vec<String>,
    #[serde(default, skip_serializing)]
    pub honest_addresses: Vec<String>,
    #[serde(default, skip_serializing)]
    pub repeat_infringing_malicious_addresses: Vec<String>,
    #[serde(default, skip_serializing)]
    pub attacker_cost_by_contract_usd: BTreeMap<String, f64>,
    #[serde(default, skip_serializing)]
    pub honest_loss_by_contract_usd: BTreeMap<String, f64>,
    #[serde(default, skip_serializing)]
    pub stuck_time_numerator_by_contract: BTreeMap<String, f64>,
    #[serde(default, skip_serializing)]
    pub stuck_time_denominator_by_contract: BTreeMap<String, f64>,
    #[serde(default, skip_serializing)]
    pub behavior_contract_denominator: i64,
    #[serde(default, skip_serializing)]
    pub behavior_contract_denominator_keys: Vec<String>,
    #[serde(default, skip_serializing)]
    pub duplicate_nft_keys_by_category: BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing)]
    pub duplicate_contract_keys_by_category: BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing)]
    pub duplicate_contract_denominator_keys: Vec<String>,
    #[serde(default, skip_serializing)]
    pub behavior_contracts_by_type: BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing)]
    pub behavior_addresses_by_type: BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing)]
    pub behavior_nfts_by_type: BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing)]
    pub behavior_buyers_by_type: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractLevelSummaryPayload {
    pub candidate_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MaliciousAddressPayload {
    pub address: String,
    pub mint_activity_observed: bool,
    pub wash_cycle_count: i64,
    #[serde(default)]
    pub wash_cycle_propagation_count: i64,
    #[serde(default)]
    pub wash_cycle_value_eth: f64,
    #[serde(default)]
    pub wash_cycle_value_usd: f64,
    pub star_out_degree: i64,
    #[serde(default)]
    pub sale_seller_count: i64,
    #[serde(default)]
    pub withdrawal_edge_count: i64,
    #[serde(default)]
    pub cashout_edge_count: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub operator_level: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operator_level_label: String,
    pub rapid_spread_contracts: Vec<String>,
    pub evidence_contracts: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NftPropagationSummaryPayload {
    pub token_count: i64,
    pub node_count: i64,
    pub edge_count: i64,
    pub mint_edge_count: i64,
    pub transfer_edge_count: i64,
    pub sale_edge_count: i64,
    pub malicious_node_count: i64,
    pub victim_node_count: i64,
    pub honest_node_count: i64,
    pub stuck_victim_node_count: i64,
    pub first_block_number: i64,
    pub last_block_number: i64,
    pub first_block_time: i64,
    pub last_block_time: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct NftPropagationNodePayload {
    pub address: String,
    pub roles: Vec<String>,
    pub minted_token_count: i64,
    pub bought_token_count: i64,
    pub sold_token_count: i64,
    pub received_transfer_count: i64,
    pub sent_transfer_count: i64,
    pub current_holding_token_count: i64,
    pub total_buy_eth: f64,
    pub total_buy_usd: f64,
    pub is_stuck_victim: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct NftPropagationEdgePayload {
    pub edge_id: String,
    pub contract_address: String,
    pub token_id: String,
    pub from_address: String,
    pub to_address: String,
    pub tx_hash: String,
    pub block_number: i64,
    pub block_time: i64,
    pub log_index: i64,
    pub event_type: String,
    pub channel: String,
    pub marketplace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payment_token_symbol: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payment_token_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seller_fee_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seller_fee_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_fee_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_fee_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub royalty_fee_eth: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub royalty_fee_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub royalty_recipient_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds_since_mint: Option<i64>,
    #[serde(
        default = "default_aggregate_count",
        skip_serializing_if = "is_one_i64"
    )]
    pub aggregate_count: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub token_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tx_hashes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_block_number: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_block_time: Option<i64>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub merged_transfer: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub underlying_channels: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NftTokenPropagationPayload {
    pub token_id: String,
    pub match_reasons: Vec<String>,
    pub minter_address: String,
    pub mint_tx_hash: String,
    pub mint_block: i64,
    pub mint_time: i64,
    pub first_transfer_time: i64,
    pub current_holder_addresses: Vec<String>,
    pub buyer_addresses: Vec<String>,
    pub seller_addresses: Vec<String>,
    pub edge_count: i64,
    pub sale_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct NftPropagationPathPayload {
    pub contract_address: String,
    pub summary: NftPropagationSummaryPayload,
    pub nodes: BTreeMap<String, NftPropagationNodePayload>,
    pub edges: Vec<NftPropagationEdgePayload>,
    pub token_paths: Vec<NftTokenPropagationPayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SingleReportPayload {
    #[serde(
        default = "single_seed_report_type",
        serialize_with = "serialize_single_seed_report_type"
    )]
    pub report_type: String,
    pub seed_contract: SeedContractPayload,
    #[serde(default)]
    pub paper_stats: PaperStatsPayload,
    #[serde(skip_serializing)]
    pub seed_collection_stats: SeedCollectionStatsPayload,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub duplicate_candidates: Vec<DuplicateCandidate>,
    #[serde(skip_serializing)]
    pub contract_level_summary: BTreeMap<String, ContractLevelSummaryPayload>,
    #[serde(skip_serializing)]
    pub duplicate_contracts: Vec<DuplicateContractPayload>,
    #[serde(skip_serializing)]
    pub legit_duplicates: Vec<DuplicateContractPayload>,
    #[serde(skip_serializing)]
    pub address_signals: BTreeMap<String, AddressSignalPayload>,
    #[serde(skip_serializing)]
    pub victim_signals: BTreeMap<String, VictimSignalPayload>,
    #[serde(skip_serializing)]
    pub infringing_tokens: Vec<InfringingTokenRecord>,
    #[serde(skip_serializing)]
    pub malicious_addresses: Vec<MaliciousAddressPayload>,
    #[serde(skip_serializing)]
    pub honest_addresses: Vec<HonestAddressPayload>,
    #[serde(skip_serializing)]
    pub secondary_sale_victim_addresses: Vec<SecondarySaleVictimAddressPayload>,
    #[serde(skip_serializing)]
    pub victim_acquisition_addresses: Vec<VictimAcquisitionAddressPayload>,
    #[serde(default, skip_serializing)]
    pub address_attributions: Vec<AddressAttributionPayload>,
    #[serde(default, skip_serializing)]
    pub contract_lifecycle_events: Vec<ContractLifecycleEventPayload>,
    #[serde(default, skip_serializing)]
    pub address_evidence_features: Vec<AddressEvidenceFeaturePayload>,
    #[serde(default, skip_serializing)]
    pub value_flow_edges: Vec<ValueFlowEdgePayload>,
    #[serde(default, skip_serializing)]
    pub content_similarity_edges: Vec<ContentSimilarityEdgePayload>,
    #[serde(default, skip_serializing)]
    pub campaign_clusters: Vec<CampaignClusterPayload>,
    #[serde(default, skip_serializing)]
    pub lifecycle_metrics: Vec<ContractLifecycleMetricPayload>,
    #[serde(default, skip_serializing)]
    pub weak_supervision_labels: Vec<WeakSupervisionLabelPayload>,
    #[serde(default, skip_serializing)]
    pub early_detection_features: Vec<EarlyDetectionFeaturePayload>,
    #[serde(default, skip_serializing)]
    pub market_events: Vec<NftMarketEventRecord>,
    #[serde(default, skip_serializing)]
    pub nft_propagation_paths: BTreeMap<String, NftPropagationPathPayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BatchSummaryPayload {
    #[serde(
        default = "batch_summary_report_type",
        serialize_with = "serialize_batch_summary_report_type"
    )]
    pub report_type: String,
    #[serde(default)]
    pub paper_stats: PaperStatsPayload,
}
