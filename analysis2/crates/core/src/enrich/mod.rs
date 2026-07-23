//! Provider HTTP clients and candidate enrichment.

pub mod alchemy;
pub mod controllers;
pub mod etherscan;
pub mod helius;
pub mod http;
pub mod legit_detect;
pub mod mint_payment;
pub mod opensea;
pub mod orchestrator;
pub mod types;
pub mod value_flow;

pub use helius::{parse_collection_address, resolve_collection_address};
pub use http::HttpClient;
pub use legit_detect::attach_relation_legit;
pub use opensea::{parse_top_collections, OpenSeaRankedItem};
pub use orchestrator::enrich_candidates;
pub use types::{
    finalize_legit_signals, ApiKeys, EvidenceBundle, EvidenceObservation, EvidenceQuality,
    EvidenceStatus, HolderRecord, HttpLimits, LegitSignals, PriceBucket, ProviderEndpoints,
    SaleEvent, TransferEvent, ValueFlowEdge, ValueFlowKind,
};
