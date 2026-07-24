//! Offline reporting: duplicate-scale aggregates, JSON/Markdown, run manifest.

pub mod aggregate;
pub mod dedup_cache;
pub mod evidence_cache;
pub mod json;
pub mod layout;
pub mod manifest;
pub mod markdown;
pub mod run;

pub use aggregate::{
    build_all_chains_duplicate_scale, build_contract_nft_map, build_duplicate_scale_rows,
    build_seed_duplicate_scale, count_scope_nfts, AllChainsRelationRef, ChainMatrixBlock,
    DuplicateScaleRow, ScopeNftCounts, SeedDuplicateScale,
};
pub use dedup_cache::{
    build_dedup_cache, default_dedup_cache_path, load_dedup_cache, rematerialize_dedup_batch,
    validate_dedup_cache, write_dedup_cache, CachedHitEdge, CachedSeedHits, DedupCacheFile,
    DedupCacheParams, DEFAULT_DEDUP_CACHE_FILE, DEDUP_CACHE_VERSION,
};
pub use evidence_cache::{
    build_evidence_cache, default_evidence_cache_path, evidence_cache_artifacts_present,
    evidence_cache_params, load_evidence_cache, load_evidence_cache_resumable,
    rematerialize_evidence, validate_evidence_cache, write_evidence_cache, EvidenceCacheFile,
    EvidenceCacheParams, EvidenceCacheSink, DEFAULT_EVIDENCE_CACHE_BATCH,
    DEFAULT_EVIDENCE_CACHE_FILE, EVIDENCE_CACHE_VERSION,
};
pub use json::{
    build_seed_dedup_report, load_seeds_json, resolve_seed_contract, seed_dir_name,
    write_dedup_outputs, DedupRunParams, SeedDedupReport, SeedRecord, SeedRelationJson,
};
pub use layout::{
    detail_candidates_dir, detail_dir, detail_seeds_dir, ensure_output_layout, intermediate_dir,
    intermediate_path, seed_report_dir, summary_dir, summary_scope_path, DETAIL_CANDIDATES_REL,
    DETAIL_DIR, INTERMEDIATE_DIR, SCOPE_ALL_CHAINS, SCOPE_CHAIN_MATRIX, SCOPE_CROSS_CHAIN,
    SCOPE_INTRA_CHAIN, SCOPE_LABEL_ALL_CHAINS, SCOPE_LABEL_CROSS_CHAIN, SUMMARY_DIR,
};
pub use manifest::{count_failed_seeds, FailureRecord, RunManifest, RunManifestSeeds};
pub use run::{
    build_run_summary, build_seed_analysis_rollup, candidate_file_name, scopes_complete_for_seed,
    write_candidate_json, write_run_outputs, CandidateRef, EconomicsUsdRollup, SeedAnalysisRollup,
    SeedFullReport,
};
