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
    #[arg(long, default_value_t = 98.0)]
    pub name_threshold: f64,
    #[arg(long, default_value_t = 0.6)]
    pub metadata_threshold: f64,
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
    #[arg(long, default_value_t = 0)]
    pub max_tokens_per_contract: usize,
    #[arg(long, default_value_t = 0)]
    pub max_recall_rows: usize,
    #[arg(long, default_value_t = 8)]
    pub alchemy_api_max_concurrency: usize,
    #[arg(
        long = "other-api-max-concurrency",
        alias = "api-max-concurrency",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_BURST
    )]
    pub other_api_max_concurrency: usize,
    #[arg(
        long = "other-api-rate-limit-refill-ms",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS
    )]
    pub other_api_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 1)]
    pub matched_contract_max_concurrency: usize,
    #[arg(long, default_value_t = 0)]
    pub duckdb_threads: usize,
    #[arg(long, default_value = "80GB")]
    pub duckdb_memory_limit: String,
    #[arg(long, default_value = "")]
    pub output: String,
    #[arg(long, default_value = "")]
    pub feature_parquet: String,
    #[arg(long, default_value = ":memory:")]
    pub feature_db: String,
}

#[derive(Args, Debug)]
pub struct BatchArgs {
    #[arg(long, default_value = "ethereum")]
    pub chain: String,
    #[arg(long)]
    pub seed_file: String,
    #[arg(long, default_value = "")]
    pub alchemy_api_key: String,
    #[arg(long, default_value = "")]
    pub alchemy_network: String,
    #[arg(long, default_value = "")]
    pub etherscan_api_key: String,
    #[arg(long, default_value = "")]
    pub opensea_api_key: String,
    #[arg(long, default_value_t = 98.0)]
    pub name_threshold: f64,
    #[arg(long, default_value_t = 0.6)]
    pub metadata_threshold: f64,
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
    #[arg(long, default_value = "../result")]
    pub output_dir: String,
    #[arg(long, default_value_t = 1)]
    pub seed_network_max_concurrency: usize,
    #[arg(long, default_value_t = 8)]
    pub alchemy_api_max_concurrency: usize,
    #[arg(
        long = "other-api-max-concurrency",
        alias = "api-max-concurrency",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_BURST
    )]
    pub other_api_max_concurrency: usize,
    #[arg(
        long = "other-api-rate-limit-refill-ms",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS
    )]
    pub other_api_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 1)]
    pub matched_contract_max_concurrency: usize,
    #[arg(long, default_value_t = 1)]
    pub seed_cpu_max_concurrency: usize,
    #[arg(long, default_value_t = 0)]
    pub duckdb_threads: usize,
    #[arg(long, default_value = "80GB")]
    pub duckdb_memory_limit: String,
    #[arg(long, default_value = "")]
    pub feature_parquet: String,
    #[arg(long, default_value = ":memory:")]
    pub feature_db: String,
    #[arg(long, default_value_t = 0)]
    pub max_recall_rows: usize,
    #[arg(long, default_value_t = 0)]
    pub max_tokens_per_contract: usize,
}

#[derive(Args, Debug)]
pub struct ExportSnapshotArgs {
    #[arg(long, default_value = "ethereum")]
    pub chain: String,
    #[arg(long)]
    pub output: String,
    #[arg(long, default_value_t = 100_000)]
    pub fetch_size: usize,
}
