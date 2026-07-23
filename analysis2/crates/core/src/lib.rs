//! analysis2 core library (engines filled in by later tasks).

pub mod analysis;
pub mod dedup;
pub mod enrich;
pub mod entity;
pub mod error;
pub mod parquet;
pub mod progress;
pub mod reporting;
pub mod seed;

pub use dedup::{
    finalize_metadata_index, finalize_name_index, query_metadata_for_seed, query_name_for_seed,
    query_uri_for_seed, CandidateRegistry, Dimension, HitEdge, HitGraph, MetadataIndex, ScopeKind,
    SeedCandidateRelation, DEFAULT_METADATA_THRESHOLD, DEFAULT_NAME_THRESHOLD,
};
pub use entity::{
    compare_token_ids, compare_token_ids_desc, finalize_name_representatives_stub, ChainId,
    ChainTotals, Contract, ContractId, CsrIndex, IdentityRow, MetadataRecord, Nft, NftId,
    ResidentStore, SourceOrder, StringId, StringPool,
};
pub use error::Analysis2Error;
pub use parquet::{load_resident_store, LoadOptions};
pub use progress::{EwmaEta, NoopProgress, ProgressObserver};
pub use reporting::{
    build_contract_nft_map, build_seed_analysis_rollup, build_seed_dedup_report, count_failed_seeds,
    count_scope_nfts, load_seeds_json, resolve_seed_contract, scopes_complete_for_seed,
    write_candidate_json, write_dedup_outputs, write_run_outputs, DedupRunParams, FailureRecord,
    ScopeNftCounts, SeedDedupReport, SeedFullReport, SeedRecord,
};
pub use seed::{
    select_seeds, select_seeds_async, write_seed_outputs, SelectSeedsOptions,
    SeedRecord as SelectedSeed,
};
pub use analysis::{
    analyze_candidate, AddressRole, BehaviorFacts, BehaviorInstance, BehaviorKind,
    CandidateAnalysis, PaperConfig,
};
pub use enrich::{
    enrich_candidates, finalize_legit_signals, ApiKeys, EvidenceBundle, EvidenceObservation,
    EvidenceQuality, EvidenceStatus, HolderRecord, HttpLimits, LegitSignals, PriceBucket,
    ProviderEndpoints, SaleEvent, TransferEvent, ValueFlowEdge, ValueFlowKind,
};
