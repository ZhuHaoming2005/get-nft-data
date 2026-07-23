//! Offline reporting: duplicate-scale aggregates, JSON/Markdown, run manifest.

pub mod aggregate;
pub mod dedup_cache;
pub mod json;
pub mod manifest;
pub mod markdown;
pub mod run;

pub use aggregate::{
    build_contract_nft_map, build_duplicate_scale_rows, build_seed_duplicate_scale, count_scope_nfts,
    ChainMatrixBlock, DuplicateScaleRow, ScopeNftCounts, SeedDuplicateScale,
};
pub use dedup_cache::{
    build_dedup_cache, default_dedup_cache_path, load_dedup_cache, rematerialize_dedup_batch,
    validate_dedup_cache, write_dedup_cache, CachedHitEdge, CachedSeedHits, DedupCacheFile,
    DedupCacheParams, DEFAULT_DEDUP_CACHE_FILE, DEDUP_CACHE_VERSION,
};
pub use json::{
    build_seed_dedup_report, load_seeds_json, resolve_seed_contract, seed_dir_name,
    write_dedup_outputs, DedupRunParams, SeedDedupReport, SeedRecord, SeedRelationJson,
};
pub use manifest::{count_failed_seeds, FailureRecord, RunManifest, RunManifestSeeds};
pub use run::{
    build_run_summary, build_seed_analysis_rollup, candidate_file_name, scopes_complete_for_seed,
    write_candidate_json, write_run_outputs, CandidateRef, EconomicsUsdRollup, SeedAnalysisRollup,
    SeedFullReport,
};
