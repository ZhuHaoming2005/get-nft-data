use crate::model::{ChainId, ContractKey, NftKey, SeedId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStatus {
    NotRequested,
    Requested,
    Complete,
    Empty,
    Truncated,
    Failed,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvent {
    pub chain: ChainId,
    pub tx_id: Arc<str>,
    pub event_index: u32,
    pub timestamp: Option<i64>,
    pub block_number: Option<u64>,
    pub kind: EventKind,
    #[serde(default)]
    pub channel: Option<ValueChannel>,
    pub from: Option<Arc<str>>,
    pub to: Option<Arc<str>>,
    #[serde(default)]
    pub fee_payer: Option<Arc<str>>,
    #[serde(default)]
    pub payment_payer: Option<Arc<str>>,
    #[serde(default)]
    pub payment_recipient: Option<Arc<str>>,
    pub nft: Option<NftKey>,
    pub native_amount: Option<i128>,
    pub usd_micros: Option<i128>,
    pub gas_native: Option<i128>,
    #[serde(default)]
    pub gas_usd_micros: Option<i128>,
    #[serde(default)]
    pub marketplace_fee_native: Option<i128>,
    #[serde(default)]
    pub marketplace_fee_usd_micros: Option<i128>,
}

impl NormalizedEvent {
    pub const fn value_channel(&self) -> ValueChannel {
        match self.channel {
            Some(channel) => channel,
            None => self.kind.default_channel(),
        }
    }

    pub const fn is_nft_sale(&self) -> bool {
        matches!(
            (self.kind, self.value_channel()),
            (EventKind::Sale, ValueChannel::SalePayment)
        )
    }

    pub const fn is_nft_propagation(&self) -> bool {
        matches!(self.kind, EventKind::Mint | EventKind::Transfer) || self.is_nft_sale()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueChannel {
    Deployment,
    MintPayment,
    Transfer,
    SalePayment,
    RoyaltyFee,
    Listing,
    Funding,
    Withdrawal,
    CashoutHop,
    ExitPayment,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Deploy,
    Mint,
    Transfer,
    Sale,
    Listing,
    Funding,
    Withdrawal,
    Cashout,
}

impl EventKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deploy => "deploy",
            Self::Mint => "mint",
            Self::Transfer => "transfer",
            Self::Sale => "sale",
            Self::Listing => "listing",
            Self::Funding => "funding",
            Self::Withdrawal => "withdrawal",
            Self::Cashout => "cashout",
        }
    }

    pub const fn default_channel(self) -> ValueChannel {
        match self {
            Self::Deploy => ValueChannel::Deployment,
            Self::Mint => ValueChannel::MintPayment,
            Self::Transfer => ValueChannel::Transfer,
            Self::Sale => ValueChannel::SalePayment,
            Self::Listing => ValueChannel::Listing,
            Self::Funding => ValueChannel::Funding,
            Self::Withdrawal => ValueChannel::Withdrawal,
            Self::Cashout => ValueChannel::CashoutHop,
        }
    }
}

impl ValueChannel {
    pub const fn compatible_with(self, kind: EventKind) -> bool {
        matches!(
            (kind, self),
            (EventKind::Deploy, Self::Deployment)
                | (EventKind::Mint, Self::MintPayment)
                | (EventKind::Transfer, Self::Transfer)
                | (EventKind::Sale, Self::SalePayment | Self::RoyaltyFee)
                | (EventKind::Listing, Self::Listing)
                | (EventKind::Funding, Self::Funding)
                | (EventKind::Withdrawal, Self::Withdrawal | Self::ExitPayment)
                | (EventKind::Cashout, Self::CashoutHop | Self::ExitPayment)
        )
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EvidenceQuality {
    pub assets: Option<EvidenceStatus>,
    pub histories: Option<EvidenceStatus>,
    pub transactions: Option<EvidenceStatus>,
    pub prices: Option<EvidenceStatus>,
    pub authority: Option<EvidenceStatus>,
    pub sale_prices_parsed: u64,
    pub sale_prices_total: u64,
    pub candidate_assets_analyzed: u64,
    pub candidate_assets_total: u64,
    pub history_assets_requested: u64,
    pub history_assets_succeeded: u64,
    pub history_assets_complete: u64,
    pub history_assets_failed: u64,
    pub history_assets_not_requested: u64,
    pub history_assets_truncated: u64,
    pub transactions_fetched: u64,
    pub transactions_provider_reported: u64,
    pub transactions_failed: u64,
    pub signature_discovery_failures: u64,
    pub transaction_detail_failures: u64,
    pub unattributed_solana_transactions: u64,
    pub unresolved_compressed_mints: u64,
    pub missing_mint_pre_balances: u64,
    pub missing_collection_authorities: u64,
    pub supplemental_query_failures: u64,
    pub failures: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationVerification {
    pub seed_id: SeedId,
    pub official_controller_continuity: bool,
    pub authorized_reissue: bool,
    pub verified_migration: bool,
    pub official_collection_relation: bool,
    pub complete: bool,
    pub evidence_keys: Vec<Arc<str>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EvidenceObservation {
    pub source: String,
    pub request_key: String,
    pub observed_at: i64,
    pub status: EvidenceStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceBundle {
    pub candidate: ContractKey,
    pub deployment_timestamp: Option<i64>,
    #[serde(default)]
    pub duplicate_content_timestamp: Option<i64>,
    pub events: Vec<NormalizedEvent>,
    pub holders: Vec<(NftKey, Arc<str>)>,
    pub controllers: Vec<Arc<str>>,
    pub relation_verifications: Vec<RelationVerification>,
    #[serde(default)]
    pub provenance: Vec<EvidenceObservation>,
    pub quality: EvidenceQuality,
}
