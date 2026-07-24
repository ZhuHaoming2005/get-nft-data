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

pub use analysis::{
    AddressRole, BehaviorFacts, BehaviorInstance, BehaviorKind, CandidateAnalysis, PaperConfig,
    analyze_candidate,
};
pub use dedup::{
    CandidateRegistry, DEFAULT_METADATA_THRESHOLD, DEFAULT_NAME_THRESHOLD, Dimension, HitEdge,
    HitGraph, MetadataIndex, MetadataQueryScratch, NameQueryScratch, ScopeKind,
    SeedCandidateRelation, UriQueryScratch, finalize_metadata_index, finalize_name_index,
    query_metadata_for_seed, query_metadata_for_seed_with_scratch, query_name_for_seed,
    query_name_for_seed_with_scratch, query_uri_for_seed, query_uri_for_seed_with_scratch,
    set_inner_query_parallel,
};
pub use enrich::{
    ApiKeys, EvidenceBundle, EvidenceObservation, EvidenceQuality, EvidenceStatus, HolderRecord,
    HttpLimits, LegitSignals, PriceBucket, ProviderEndpoints, SaleEvent, TransferEvent,
    ValueFlowEdge, ValueFlowKind, enrich_candidates, enrich_candidates_with_hook,
    finalize_legit_signals,
};
pub use entity::{
    ChainId, ChainTotals, Contract, ContractId, CsrIndex, IdentityRow, MetadataRecord, Nft, NftId,
    ResidentStore, SourceOrder, StringId, StringPool, compare_token_ids, compare_token_ids_desc,
    finalize_name_representatives_stub,
};
pub use error::Analysis2Error;
pub use parquet::{
    load_resident_store, load_resident_store_uri_ready, LoadOptions, PendingDedupLoad,
};
pub use progress::{EwmaEta, NoopProgress, ProgressObserver};
pub use reporting::{
    build_all_chains_duplicate_scale, build_contract_nft_map, build_dedup_cache,
    build_evidence_cache, build_seed_analysis_rollup, build_seed_dedup_report, count_failed_seeds,
    count_scope_nfts, default_dedup_cache_path, default_evidence_cache_path, detail_candidates_dir,
    detail_dir, ensure_output_layout, evidence_cache_artifacts_present, evidence_cache_params,
    intermediate_dir, load_dedup_cache, load_evidence_cache, load_evidence_cache_resumable,
    load_seeds_json, rematerialize_dedup_batch, rematerialize_evidence, resolve_seed_contract,
    scopes_complete_for_seed, serialize_candidate_json, summary_dir, validate_dedup_cache,
    validate_evidence_cache, write_candidate_json, write_candidate_json_bytes, write_dedup_cache,
    write_dedup_outputs, write_evidence_cache, write_run_outputs, CachedHitEdge, CachedSeedHits,
    DedupCacheFile, DedupCacheParams, DedupRunParams, EvidenceCacheFile, EvidenceCacheParams,
    EvidenceCacheSink, FailureRecord, ScopeNftCounts, SeedDedupReport, SeedFullReport, SeedRecord,
    DEFAULT_DEDUP_CACHE_FILE, DEFAULT_EVIDENCE_CACHE_BATCH, DEFAULT_EVIDENCE_CACHE_FILE,
    DETAIL_CANDIDATES_REL, DETAIL_DIR, DEDUP_CACHE_VERSION, EVIDENCE_CACHE_VERSION, INTERMEDIATE_DIR,
    SCOPE_ALL_CHAINS, SCOPE_CHAIN_MATRIX, SCOPE_CROSS_CHAIN, SCOPE_INTRA_CHAIN, SUMMARY_DIR,
    candidate_json_rel_path,
};
pub use seed::{
    SeedRecord as SelectedSeed, SelectSeedsOptions, select_seeds, select_seeds_async,
    write_seed_outputs,
};
