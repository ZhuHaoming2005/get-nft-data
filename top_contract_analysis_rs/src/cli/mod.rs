use clap::{Args, Parser, Subcommand};

use crate::api::{DEFAULT_OTHER_API_RATE_LIMIT_BURST, DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS};

fn parse_name_threshold(value: &str) -> Result<f64, String> {
    parse_finite_threshold(value, 0.0, 100.0, "name threshold")
}

fn parse_metadata_threshold(value: &str) -> Result<f64, String> {
    parse_finite_threshold(value, 0.0, 1.0, "metadata threshold")
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(parsed)
}

fn parse_finite_threshold(
    value: &str,
    minimum: f64,
    maximum: f64,
    label: &str,
) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| format!("invalid {label} {value:?}: {error}"))?;
    if !parsed.is_finite() || !(minimum..=maximum).contains(&parsed) {
        return Err(format!(
            "{label} must be finite and between {minimum} and {maximum} inclusive"
        ));
    }
    Ok(parsed)
}

#[derive(Parser, Debug)]
pub struct TopContractAnalysisCli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Analyze(AnalyzeArgs),
    Batch(BatchArgs),
    PrepareFeatures(PrepareFeaturesArgs),
    ExportSnapshot(ExportSnapshotArgs),
}

#[derive(Args, Debug)]
pub struct PrepareFeaturesArgs {
    #[arg(long)]
    pub feature_parquet: Vec<String>,
    #[arg(long, required = true)]
    pub feature_db: String,
    /// Resume derived-table preparation from the last committed authoritative
    /// import without reopening the Parquet inputs.
    #[arg(long, conflicts_with = "restart_prepare")]
    pub prepare_only: bool,
    /// Discard an unfinished prepare journal and start a new authoritative
    /// import. Parquet inputs are required with this option.
    #[arg(long, conflicts_with = "prepare_only")]
    pub restart_prepare: bool,
    #[arg(long)]
    pub allow_in_memory_feature_db: bool,
    #[arg(long, default_value_t = 96, value_parser = parse_positive_usize)]
    pub duckdb_threads: usize,
    #[arg(long, default_value_t = 96, value_parser = parse_positive_usize)]
    pub rayon_threads: usize,
    #[arg(long, default_value_t = 0)]
    pub physical_cores: usize,
    #[arg(long, default_value = "300GB")]
    pub duckdb_memory_limit: String,
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
    #[arg(long, default_value_t = 95.0, value_parser = parse_name_threshold)]
    pub name_threshold: f64,
    #[arg(long, default_value_t = 0.6, value_parser = parse_metadata_threshold)]
    pub metadata_threshold: f64,
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
    #[arg(long, default_value_t = 200)]
    pub max_tokens_per_contract: usize,
    #[arg(long, default_value_t = 0)]
    pub max_recall_rows: usize,
    #[arg(long, default_value = "24GB")]
    pub max_snapshot_bytes_per_seed: String,
    #[arg(long, default_value_t = 100_000)]
    pub max_candidate_contracts_per_seed: usize,
    #[arg(long, default_value_t = 2_000_000)]
    pub max_selected_rows_per_seed: usize,
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    pub alchemy_api_max_concurrency: usize,
    #[arg(
        long = "other-api-max-concurrency",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_BURST,
        value_parser = parse_positive_usize
    )]
    pub other_api_max_concurrency: usize,
    #[arg(
        long = "other-api-rate-limit-refill-ms",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS,
        value_parser = parse_positive_u64
    )]
    pub other_api_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 8, value_parser = parse_positive_usize)]
    pub matched_contract_max_concurrency: usize,
    /// Total DuckDB workers across all read connections.
    #[arg(long, default_value_t = 64, value_parser = parse_positive_usize)]
    pub duckdb_threads: usize,
    /// Rayon workers used by exact Name/Metadata scoring.
    #[arg(long, default_value_t = 96, value_parser = parse_positive_usize)]
    pub rayon_threads: usize,
    /// Parallel read-only connections. Threads and memory are divided across
    /// them so this does not multiply the configured DuckDB envelope.
    #[arg(long, default_value_t = 2, value_parser = parse_positive_usize)]
    pub duckdb_read_connections: usize,
    /// Physical core count. When set, caps DuckDB and overrides Rayon workers.
    #[arg(long, default_value_t = 0)]
    pub physical_cores: usize,
    #[arg(long, default_value = "96GB")]
    pub duckdb_memory_limit: String,
    /// Combined resident budget for cached Rust name and metadata recall indexes.
    #[arg(long, default_value = "260GB")]
    pub recall_index_memory_limit: String,
    #[arg(long, default_value = "")]
    pub output: String,
    #[arg(long, default_value = "")]
    pub feature_parquet: String,
    #[arg(long, default_value = "features.duckdb")]
    pub feature_db: String,
    #[arg(long, default_value = "")]
    pub helius_api_key: String,
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    pub helius_api_max_concurrency: usize,
    #[arg(long, default_value_t = 100, value_parser = parse_positive_u64)]
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
    /// Unix timestamp used for time-dependent paper metrics. 0 = current run time.
    #[arg(long, default_value_t = 0)]
    pub paper_analysis_timestamp: i64,
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
    #[arg(long, default_value_t = 95.0, value_parser = parse_name_threshold)]
    pub name_threshold: f64,
    #[arg(long, default_value_t = 0.6, value_parser = parse_metadata_threshold)]
    pub metadata_threshold: f64,
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
    #[arg(long, default_value = "../result")]
    pub output_dir: String,
    /// Start a fresh run even when a matching incomplete run manifest exists.
    #[arg(long)]
    pub refresh_scoped_cache: bool,
    #[arg(long, default_value_t = 2, value_parser = parse_positive_usize)]
    pub seed_network_max_concurrency: usize,
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    pub alchemy_api_max_concurrency: usize,
    #[arg(
        long = "other-api-max-concurrency",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_BURST,
        value_parser = parse_positive_usize
    )]
    pub other_api_max_concurrency: usize,
    #[arg(
        long = "other-api-rate-limit-refill-ms",
        default_value_t = DEFAULT_OTHER_API_RATE_LIMIT_INTERVAL_MS,
        value_parser = parse_positive_u64
    )]
    pub other_api_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 8, value_parser = parse_positive_usize)]
    pub matched_contract_max_concurrency: usize,
    #[arg(long, default_value_t = 2, value_parser = parse_positive_usize)]
    pub seed_cpu_max_concurrency: usize,
    /// Total DuckDB workers across all read connections.
    #[arg(long, default_value_t = 64, value_parser = parse_positive_usize)]
    pub duckdb_threads: usize,
    /// Rayon workers used by exact Name/Metadata scoring.
    #[arg(long, default_value_t = 96, value_parser = parse_positive_usize)]
    pub rayon_threads: usize,
    /// Parallel read-only connections. Threads and memory are divided across
    /// them so this does not multiply the configured DuckDB envelope.
    #[arg(long, default_value_t = 2, value_parser = parse_positive_usize)]
    pub duckdb_read_connections: usize,
    /// Physical core count. When set, caps DuckDB and overrides Rayon workers.
    #[arg(long, default_value_t = 0)]
    pub physical_cores: usize,
    #[arg(long, default_value = "96GB")]
    pub duckdb_memory_limit: String,
    /// Combined resident budget for cached Rust name and metadata recall indexes.
    #[arg(long, default_value = "260GB")]
    pub recall_index_memory_limit: String,
    #[arg(long)]
    pub feature_parquet: Vec<String>,
    #[arg(long, default_value = "features.duckdb")]
    pub feature_db: String,
    #[arg(long, default_value = "")]
    pub helius_api_key: String,
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    pub helius_api_max_concurrency: usize,
    #[arg(long, default_value_t = 100, value_parser = parse_positive_u64)]
    pub helius_rate_limit_refill_ms: u64,
    #[arg(long, default_value_t = 100)]
    pub max_history_transactions_per_asset: usize,
    #[arg(long, default_value_t = 10_000)]
    pub max_history_transactions_per_collection: usize,
    #[arg(long, default_value_t = 10_000)]
    pub max_helius_assets_per_collection: usize,
    #[arg(long, default_value_t = 0)]
    pub max_recall_rows: usize,
    #[arg(long, default_value_t = 200)]
    pub max_tokens_per_contract: usize,
    #[arg(long, default_value = "24GB")]
    pub max_snapshot_bytes_per_seed: String,
    #[arg(long, default_value_t = 100_000)]
    pub max_candidate_contracts_per_seed: usize,
    #[arg(long, default_value_t = 2_000_000)]
    pub max_selected_rows_per_seed: usize,
    #[arg(long, default_value_t = 2)]
    pub paper_min_cycle_size: usize,
    #[arg(long, default_value_t = 3)]
    pub paper_min_path_length: usize,
    #[arg(long, default_value_t = 3)]
    pub paper_center_fanout_threshold: usize,
    #[arg(long, default_value_t = 0.1)]
    pub paper_concentration_top_pct: f64,
    /// Unix timestamp used for time-dependent paper metrics and cache identity. 0 = current run time.
    #[arg(long, default_value_t = 0)]
    pub paper_analysis_timestamp: i64,
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
