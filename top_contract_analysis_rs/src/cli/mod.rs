use clap::{Args, Parser, Subcommand};

use crate::api::{DEFAULT_OTHER_API_RATE_LIMIT_BURST, DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS};

#[derive(Parser, Debug)]
pub struct TopContractAnalysisCli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Analyze(AnalyzeArgs),
    Batch(BatchArgs),
    ExportSnapshot(ExportSnapshotArgs),
}

#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    #[arg(long, default_value = "ethereum")]
    pub chain: String,
    #[arg(long)]
    pub seed_contract_address: String,
    #[arg(long, default_value = "")]
    pub alchemy_api_key: String,
    #[arg(long, default_value = "")]
    pub alchemy_network: String,
    #[arg(long, default_value = "")]
    pub etherscan_api_key: String,
    #[arg(long, default_value = "")]
    pub opensea_api_key: String,
    #[arg(long, default_value_t = 95.0)]
    pub name_threshold: f64,
    #[arg(long, default_value_t = 0.6)]
    pub metadata_threshold: f64,
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
    #[arg(long, default_value_t = 0)]
    pub max_tokens_per_contract: usize,
    #[arg(long, default_value_t = 0)]
    pub max_recall_rows: usize,
    #[arg(long, default_value_t = 16)]
    pub alchemy_api_max_concurrency: usize,
    #[arg(
        long = "other-api-max-concurrency",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_BURST
    )]
    pub other_api_max_concurrency: usize,
    #[arg(
        long = "other-api-rate-limit-refill-ms",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS
    )]
    pub other_api_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 4)]
    pub matched_contract_max_concurrency: usize,
    /// DuckDB worker threads. 0 = auto = SMT (all logical cores). On a
    /// 32-physical-core SMT host this defaults to 64; pass `--physical-cores
    /// 32` to pin DuckDB + Rayon to physical cores for the recall hash joins.
    #[arg(long, default_value_t = 0)]
    pub duckdb_threads: usize,
    /// Physical core count. When set, overrides `--duckdb-threads` and pins
    /// DuckDB + Rayon to N threads.
    #[arg(long, default_value_t = 0)]
    pub physical_cores: usize,
    #[arg(long, default_value = "150GB")]
    pub duckdb_memory_limit: String,
    #[arg(long, default_value = "")]
    pub output: String,
    #[arg(long, default_value = "")]
    pub feature_parquet: String,
    #[arg(long, default_value = ":memory:")]
    pub feature_db: String,
    #[arg(long, default_value = "")]
    pub helius_api_key: String,
    #[arg(long, default_value_t = 4)]
    pub helius_api_max_concurrency: usize,
    #[arg(long, default_value_t = 100)]
    pub helius_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 100)]
    pub max_history_transactions_per_asset: usize,
    #[arg(long, default_value_t = 10_000)]
    pub max_history_transactions_per_collection: usize,
    #[arg(long, default_value_t = 10_000)]
    pub max_helius_assets_per_collection: usize,
    #[arg(long, default_value_t = 2)]
    pub paper_min_cycle_size: usize,
    #[arg(long, default_value_t = 3)]
    pub paper_min_path_length: usize,
    #[arg(long, default_value_t = 3)]
    pub paper_center_fanout_threshold: usize,
    #[arg(long, default_value_t = 0.1)]
    pub paper_concentration_top_pct: f64,
}

#[derive(Args, Debug)]
pub struct BatchArgs {
    #[arg(long)]
    pub seed_file: String,
    #[arg(long, default_value = "")]
    pub alchemy_api_key: String,
    #[arg(long)]
    pub alchemy_network: Vec<String>,
    #[arg(long, default_value = "")]
    pub etherscan_api_key: String,
    #[arg(long, default_value = "")]
    pub opensea_api_key: String,
    #[arg(long, default_value_t = 95.0)]
    pub name_threshold: f64,
    #[arg(long, default_value_t = 0.6)]
    pub metadata_threshold: f64,
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
    #[arg(long, default_value = "../result")]
    pub output_dir: String,
    #[arg(long, default_value_t = 2)]
    pub seed_network_max_concurrency: usize,
    #[arg(long, default_value_t = 16)]
    pub alchemy_api_max_concurrency: usize,
    #[arg(
        long = "other-api-max-concurrency",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_BURST
    )]
    pub other_api_max_concurrency: usize,
    #[arg(
        long = "other-api-rate-limit-refill-ms",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS
    )]
    pub other_api_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 4)]
    pub matched_contract_max_concurrency: usize,
    #[arg(long, default_value_t = 1)]
    pub seed_cpu_max_concurrency: usize,
    /// DuckDB worker threads. 0 = auto = SMT (all logical cores). On a
    /// 32-physical-core SMT host this defaults to 64; pass `--physical-cores
    /// 32` to pin DuckDB + Rayon to physical cores for the recall hash joins.
    #[arg(long, default_value_t = 0)]
    pub duckdb_threads: usize,
    /// Physical core count. When set, overrides `--duckdb-threads` and pins
    /// DuckDB + Rayon to N threads.
    #[arg(long, default_value_t = 0)]
    pub physical_cores: usize,
    #[arg(long, default_value = "150GB")]
    pub duckdb_memory_limit: String,
    #[arg(long)]
    pub feature_parquet: Vec<String>,
    #[arg(long, default_value = ":memory:")]
    pub feature_db: String,
    #[arg(long, default_value = "")]
    pub helius_api_key: String,
    #[arg(long, default_value_t = 4)]
    pub helius_api_max_concurrency: usize,
    #[arg(long, default_value_t = 100)]
    pub helius_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 100)]
    pub max_history_transactions_per_asset: usize,
    #[arg(long, default_value_t = 10_000)]
    pub max_history_transactions_per_collection: usize,
    #[arg(long, default_value_t = 10_000)]
    pub max_helius_assets_per_collection: usize,
    #[arg(long, default_value_t = 0)]
    pub max_recall_rows: usize,
    #[arg(long, default_value_t = 0)]
    pub max_tokens_per_contract: usize,
    #[arg(long, default_value_t = 2)]
    pub paper_min_cycle_size: usize,
    #[arg(long, default_value_t = 3)]
    pub paper_min_path_length: usize,
    #[arg(long, default_value_t = 3)]
    pub paper_center_fanout_threshold: usize,
    #[arg(long, default_value_t = 0.1)]
    pub paper_concentration_top_pct: f64,
}

#[derive(Args, Debug)]
pub struct ExportSnapshotArgs {
    #[arg(long, default_value = "ethereum")]
    pub chain: String,
    #[arg(long)]
    pub output: String,
    #[arg(long, default_value_t = 100_000)]
    pub fetch_size: usize,
    #[arg(long)]
    pub start_block: Option<i64>,
    #[arg(long)]
    pub end_block: Option<i64>,
}
