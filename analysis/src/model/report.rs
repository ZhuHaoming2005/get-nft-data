use crate::model::{CandidateId, ContractKey, EvidenceQuality, NftKey, NftSelection, SeedId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "classification", rename_all = "snake_case")]
pub enum RelationClassification {
    SuspectedDuplicate { legit_verification_complete: bool },
    LegitDuplicate { evidence: Vec<std::sync::Arc<str>> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationLabel {
    pub seed_id: SeedId,
    pub candidate_id: CandidateId,
    pub classification: RelationClassification,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LifecycleFacts {
    pub deployment_timestamp: Option<i64>,
    pub first_activity_timestamp: Option<i64>,
    pub first_mint_timestamp: Option<i64>,
    pub first_transfer_timestamp: Option<i64>,
    pub first_sale_timestamp: Option<i64>,
    pub first_victim_timestamp: Option<i64>,
    pub deployment_to_first_transfer_seconds: Option<i64>,
    pub deployment_to_first_sale_seconds: Option<i64>,
    pub deployment_to_first_victim_seconds: Option<i64>,
    pub first_activity_to_first_victim_seconds: Option<i64>,
    pub first_victim_holding_seconds: Option<i64>,
    pub early_signal_categories: Vec<String>,
    pub early_signal_positive: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PropagationFacts {
    pub propagation_edge_count: u64,
    pub mint_edge_count: u64,
    pub transfer_edge_count: u64,
    pub sale_edge_count: u64,
    pub funding_edge_count: u64,
    pub withdrawal_edge_count: u64,
    pub cashout_edge_count: u64,
    pub gross_revenue_edge_count: u64,
    pub operator_revenue_edge_count: u64,
    pub marketplace_fee_edge_count: u64,
    pub revenue_backflow_edge_count: u64,
    pub malicious_address_count: u64,
    pub victim_address_count: u64,
    pub likely_honest_address_count: u64,
    pub currently_holding_victim_address_count: u64,
    pub max_value_receiver: Option<Arc<str>>,
    pub max_value_receiver_native: i128,
    pub max_value_receiver_usd_micros: i128,
    pub max_value_receiver_share: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EconomicFacts {
    pub gross_revenue_native: i128,
    pub gross_revenue_usd_micros: i128,
    pub marketplace_fee_native: i128,
    pub marketplace_fee_usd_micros: i128,
    pub funding_native: i128,
    pub funding_usd_micros: i128,
    pub withdrawal_native: i128,
    pub withdrawal_usd_micros: i128,
    pub revenue_backflow_native: i128,
    pub revenue_backflow_usd_micros: i128,
    pub setup_gas_native: i128,
    pub setup_gas_usd_micros: i128,
    pub lure_gas_native: i128,
    pub lure_gas_usd_micros: i128,
    pub exit_gas_native: i128,
    pub exit_gas_usd_micros: i128,
    pub operator_output_native: i128,
    pub operator_output_usd_micros: i128,
    pub secondary_sale_loss_native: i128,
    pub secondary_sale_loss_usd_micros: i128,
    pub paid_mint_loss_native: i128,
    pub paid_mint_loss_usd_micros: i128,
    pub honest_loss_native: i128,
    pub honest_loss_usd_micros: i128,
    pub stuck_nft_count: u64,
    pub stuck_time_numerator_seconds: i128,
    pub stuck_time_denominator_seconds: i128,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GasStage {
    Setup,
    Lure,
    Exit,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GasEvidenceKind {
    AttributedOperatorFeePayer,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GasCostRecord {
    pub chain: crate::model::ChainId,
    pub stage: GasStage,
    pub channel: crate::model::ValueChannel,
    pub transaction: Arc<str>,
    pub gas_payer: Arc<str>,
    pub gas_native: i128,
    pub gas_usd_micros: i128,
    pub from_role: Option<AddressRoleKind>,
    pub to_role: Option<AddressRoleKind>,
    pub evidence_type: GasEvidenceKind,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BehaviorFacts {
    pub wash_cycles: u64,
    pub pump_and_exit: u64,
    pub sybil_distribution: u64,
    pub fraud_revenue: u64,
    pub poisoning: u64,
    pub layered_transfer: u64,
    pub inventory_concentration: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BehaviorKind {
    #[default]
    WashTrading,
    PumpAndExit,
    SybilDistribution,
    FraudRevenue,
    Poisoning,
    LayeredTransfer,
    InventoryConcentration,
}

impl BehaviorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WashTrading => "wash_trading",
            Self::PumpAndExit => "pump_and_exit",
            Self::SybilDistribution => "sybil_distribution",
            Self::FraudRevenue => "fraud_revenue",
            Self::Poisoning => "poisoning",
            Self::LayeredTransfer => "layered_transfer",
            Self::InventoryConcentration => "inventory_concentration",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BehaviorInstance {
    pub kind: BehaviorKind,
    pub addresses: Vec<Arc<str>>,
    pub nfts: Vec<NftKey>,
    pub transactions: Vec<Arc<str>>,
    pub linked_buyers: Vec<Arc<str>>,
    pub edge_count: u64,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub native_value: i128,
    pub usd_micros: i128,
    pub linked_loss_native: i128,
    pub linked_loss_usd_micros: i128,
    pub gini_nft_count: Option<f64>,
    pub gini_token_transaction_count: Option<f64>,
    pub fan_out: Option<u64>,
    pub path_length: Option<u64>,
    pub low_value_hops: Option<u64>,
    pub source_address_count: Option<u64>,
    pub nft_share: Option<f64>,
    pub value_share: Option<f64>,
    pub exit_delay_seconds: Option<i64>,
    pub exit_to_internal_price_ratio: Option<f64>,
    pub exit_to_cycle_nft_ratio: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddressEvidence {
    pub evidence_type: AddressEvidenceKind,
    pub related_contract: ContractKey,
    pub token: Option<NftKey>,
    pub transaction: Option<Arc<str>>,
    pub weight: f64,
    pub confidence: f64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressEvidenceKind {
    ControllerOrAuthority,
    CurrentHolder,
    EventSender,
    EventRecipient,
    Deployment,
    FundingReceived,
    WithdrawalOrCashout,
    PaidAcquisition,
    SubsequentPropagation,
    MaliciousSaleCycle,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddressAttribution {
    pub role: AddressRoleKind,
    pub evidence: Vec<AddressEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressRoleKind {
    SuspectedOperator,
    SuspectedColluder,
    LikelyVictim,
    CorruptedVictim,
    Neutral,
}

impl AddressRoleKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SuspectedOperator => "suspected_operator",
            Self::SuspectedColluder => "suspected_colluder",
            Self::LikelyVictim => "likely_victim",
            Self::CorruptedVictim => "corrupted_victim",
            Self::Neutral => "neutral",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateFacts {
    pub candidate: ContractKey,
    pub analysis_timestamp: i64,
    pub lifecycle: LifecycleFacts,
    pub propagation: PropagationFacts,
    pub economics: EconomicFacts,
    pub gas_cost_records: Vec<GasCostRecord>,
    pub behaviors: BehaviorFacts,
    pub behavior_instances: Vec<BehaviorInstance>,
    pub address_count: u64,
    pub nft_count: u64,
    pub transaction_count: u64,
    pub event_count: u64,
    pub event_kind_counts: BTreeMap<String, u64>,
    pub address_attributions: BTreeMap<std::sync::Arc<str>, AddressAttribution>,
    pub honest_buyers: Vec<HonestBuyerFact>,
    pub quality: EvidenceQuality,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HonestBuyerFact {
    pub address: Arc<str>,
    pub role: AddressRoleKind,
    pub held_nfts: Vec<NftKey>,
    pub acquisition_transactions: Vec<Arc<str>>,
    pub paid_native: i128,
    pub paid_usd_micros: i128,
    pub first_purchase_timestamp: Option<i64>,
    pub first_activity_to_first_purchase_seconds: Option<i64>,
    pub holding_seconds: Option<i64>,
    pub linked_behaviors: Vec<BehaviorKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationDelta {
    pub seed_id: SeedId,
    pub seed_chain: crate::model::ChainId,
    pub candidate_id: CandidateId,
    pub candidate: ContractKey,
    pub selection: NftSelection,
    pub suspected: bool,
    pub economics: EconomicFacts,
    pub behaviors: BehaviorFacts,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateDelta {
    pub candidate_id: CandidateId,
    pub analysis_complete: bool,
    pub relation_deltas: Vec<RelationDelta>,
    pub suspected_economics: EconomicFacts,
    pub suspected_behaviors: BehaviorFacts,
    pub behavior_entities: Vec<BehaviorEntityDelta>,
    /// Suspected (or mixed-elevated) facts keyed by `(seed_chain, candidate_chain)`.
    pub matrix_suspected:
        BTreeMap<(crate::model::ChainId, crate::model::ChainId), ScopedSuspectedBundle>,
    pub candidate_quality: EvidenceQuality,
    pub global_address_roles: Vec<(crate::model::GlobalAddressId, AddressRoleKind)>,
    pub global_nft_ids: Vec<crate::model::GlobalNftId>,
    pub global_transaction_ids: Vec<crate::model::GlobalTxId>,
}

/// Per-matrix-cell suspected economics/behaviors (selection union for that cell).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScopedSuspectedBundle {
    pub economics: EconomicFacts,
    pub behaviors: BehaviorFacts,
    pub behavior_entities: Vec<BehaviorEntityDelta>,
    pub address_roles: Vec<(crate::model::GlobalAddressId, AddressRoleKind)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BehaviorEntityDelta {
    pub kind: BehaviorKind,
    pub addresses: Vec<crate::model::GlobalAddressId>,
    pub nfts: Vec<crate::model::GlobalNftId>,
    pub linked_buyers: Vec<crate::model::GlobalAddressId>,
    pub linked_loss_native: i128,
    pub linked_loss_usd_micros: i128,
}
