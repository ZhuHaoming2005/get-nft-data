use std::sync::Arc;

use clap::Parser;
#[cfg(feature = "export-snapshot")]
use postgres::{Client, NoTls};
use tokio::runtime::Runtime;
use top_contract_analysis_rs::analysis::multichain::parse_alchemy_networks;
use top_contract_analysis_rs::analysis::paper_stats::PaperStatsConfig;
use top_contract_analysis_rs::analysis::read_seed_contracts;
use top_contract_analysis_rs::analysis::{
    analyze_seed_contract, run_multichain_batch, AnalysisDeps, AnalyzeRequest, HeliusApiConfig,
    MultiChainBatchRequest, RealApi,
};
use top_contract_analysis_rs::cli::{Command, TopContractAnalysisCli};
#[cfg(feature = "export-snapshot")]
use top_contract_analysis_rs::config::postgres_connection_config;
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::Chain;
use top_contract_analysis_rs::progress::{
    create_batch_progress_reporter, create_single_seed_progress_reporter,
    NoopBatchProgressReporter, NoopProgressReporter,
};
use top_contract_analysis_rs::reporting::write_default_outputs;
#[cfg(feature = "export-snapshot")]
use top_contract_analysis_rs::store::{export_chain_snapshot_to_parquet, SnapshotBlockRange};
use top_contract_analysis_rs::store::{DuckDbFeatureStore, DuckDbResourceOptions};

#[cfg(feature = "export-snapshot")]
fn connect_postgres_from_constants() -> Result<Client, AppError> {
    let config = postgres_connection_config();
    Client::connect(&config, NoTls).map_err(AppError::from)
}

/// Resolve the shared DuckDB/Rayon thread count from `--physical-cores` /
/// `--duckdb-threads`. `--physical-cores` wins when set; otherwise 0 means
/// SMT (all logical cores, the Rayon/DuckDB default). When an explicit count
/// is given, also pin the global Rayon pool to it so DuckDB and Rayon share a
/// single thread budget rather than both racing to fill every logical core.
fn resolve_resource_threads(physical_cores: usize, duckdb_threads: usize) -> usize {
    let effective = if physical_cores > 0 {
        physical_cores
    } else {
        duckdb_threads
    };
    if effective > 0 {
        // Best-effort: if the global pool was already initialized this errors,
        // in which case Rayon keeps its default. Called once at startup before
        // any `par_iter`, so it normally succeeds.
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(effective)
            .build_global();
    }
    effective
}

fn main() -> Result<(), AppError> {
    let command = TopContractAnalysisCli::parse();
    match command.command {
        Command::Analyze(args) => Runtime::new()?.block_on(async move {
            let analyze_chain = args.chain.parse::<Chain>()?;
            let duckdb_threads = resolve_resource_threads(args.physical_cores, args.duckdb_threads);
            let duckdb_options =
                DuckDbResourceOptions::from_cli(duckdb_threads, &args.duckdb_memory_limit)?;
            let feature_store =
                DuckDbFeatureStore::new_with_options(&args.feature_db, duckdb_options)?;
            if !args.feature_parquet.trim().is_empty() {
                feature_store.load_parquet_dataset_if_chain_missing(
                    analyze_chain.as_str(),
                    &args.feature_parquet,
                )?;
            }
            let api = RealApi::new_with_helius(
                args.timeout,
                args.alchemy_api_max_concurrency,
                args.other_api_max_concurrency,
                args.other_api_rate_limit_refill_ms,
                HeliusApiConfig {
                    max_concurrency: args.helius_api_max_concurrency,
                    rate_limit_refill_ms: args.helius_rate_limit_refill_ms,
                    api_key: &args.helius_api_key,
                    max_history_transactions_per_asset: args.max_history_transactions_per_asset,
                    max_history_transactions_per_collection: args
                        .max_history_transactions_per_collection,
                    max_assets_per_collection: args.max_helius_assets_per_collection,
                },
            )?;
            let deps = AnalysisDeps {
                api: Arc::new(api),
                feature_store: Arc::new(feature_store),
                progress: create_single_seed_progress_reporter(&args.seed_contract_address),
                batch_progress: Arc::new(NoopBatchProgressReporter),
            };
            let payload = analyze_seed_contract(
                AnalyzeRequest {
                    chain: analyze_chain.to_string(),
                    seed_contract_address: analyze_chain
                        .normalize_identity(&args.seed_contract_address),
                    alchemy_api_key: args.alchemy_api_key,
                    alchemy_network: if args.alchemy_network.trim().is_empty() {
                        None
                    } else {
                        Some(args.alchemy_network)
                    },
                    etherscan_api_key: args.etherscan_api_key,
                    opensea_api_key: args.opensea_api_key,
                    name_threshold: args.name_threshold,
                    metadata_threshold: args.metadata_threshold,
                    timeout_seconds: args.timeout,
                    api_max_concurrency: args
                        .alchemy_api_max_concurrency
                        .saturating_add(args.other_api_max_concurrency)
                        .max(1),
                    matched_contract_max_concurrency: args.matched_contract_max_concurrency,
                    max_tokens_per_contract: args.max_tokens_per_contract,
                    max_recall_rows: args.max_recall_rows,
                    paper_stats_config: PaperStatsConfig {
                        min_cycle_size: args.paper_min_cycle_size,
                        min_path_length: args.paper_min_path_length,
                        center_fanout_threshold: args.paper_center_fanout_threshold,
                        concentration_top_pct: args.paper_concentration_top_pct,
                        ..PaperStatsConfig::default()
                    },
                },
                &deps,
            )
            .await?;
            write_default_outputs(&payload, &args.output)?;
            Ok(())
        }),
        Command::Batch(args) => Runtime::new()?.block_on(async move {
            let seed_contracts = read_seed_contracts(std::path::Path::new(&args.seed_file))?;
            let seed_labels = seed_contracts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let duckdb_threads = resolve_resource_threads(args.physical_cores, args.duckdb_threads);
            let duckdb_options =
                DuckDbResourceOptions::from_cli(duckdb_threads, &args.duckdb_memory_limit)?;
            let feature_store =
                DuckDbFeatureStore::new_with_options(&args.feature_db, duckdb_options)?;
            feature_store.load_parquet_datasets_auto(&args.feature_parquet)?;
            for chain in Chain::ALL {
                if !feature_store.has_chain_rows(chain.as_str())? {
                    return Err(AppError::InvalidData(format!(
                        "feature store does not contain required cross-chain snapshot for {chain}"
                    )));
                }
            }
            let api = RealApi::new_with_helius(
                args.timeout,
                args.alchemy_api_max_concurrency,
                args.other_api_max_concurrency,
                args.other_api_rate_limit_refill_ms,
                HeliusApiConfig {
                    max_concurrency: args.helius_api_max_concurrency,
                    rate_limit_refill_ms: args.helius_rate_limit_refill_ms,
                    api_key: &args.helius_api_key,
                    max_history_transactions_per_asset: args.max_history_transactions_per_asset,
                    max_history_transactions_per_collection: args
                        .max_history_transactions_per_collection,
                    max_assets_per_collection: args.max_helius_assets_per_collection,
                },
            )?;
            let deps = AnalysisDeps {
                api: Arc::new(api),
                feature_store: Arc::new(feature_store),
                progress: Arc::new(NoopProgressReporter),
                batch_progress: create_batch_progress_reporter(
                    &seed_labels,
                    args.seed_network_max_concurrency,
                ),
            };
            let output_dir = std::path::PathBuf::from(&args.output_dir);
            let payload = run_multichain_batch(
                MultiChainBatchRequest {
                    seed_file: std::path::PathBuf::from(args.seed_file),
                    output_dir: output_dir.clone(),
                    alchemy_api_key: args.alchemy_api_key,
                    alchemy_networks: parse_alchemy_networks(&args.alchemy_network)?,
                    etherscan_api_key: args.etherscan_api_key,
                    opensea_api_key: args.opensea_api_key,
                    name_threshold: args.name_threshold,
                    metadata_threshold: args.metadata_threshold,
                    timeout_seconds: args.timeout,
                    api_max_concurrency: args
                        .alchemy_api_max_concurrency
                        .saturating_add(args.other_api_max_concurrency)
                        .max(1),
                    matched_contract_max_concurrency: args.matched_contract_max_concurrency,
                    seed_network_max_concurrency: args.seed_network_max_concurrency,
                    seed_cpu_max_concurrency: args.seed_cpu_max_concurrency,
                    max_tokens_per_contract: args.max_tokens_per_contract,
                    max_recall_rows: args.max_recall_rows,
                    paper_stats_config: PaperStatsConfig {
                        min_cycle_size: args.paper_min_cycle_size,
                        min_path_length: args.paper_min_path_length,
                        center_fanout_threshold: args.paper_center_fanout_threshold,
                        concentration_top_pct: args.paper_concentration_top_pct,
                        ..PaperStatsConfig::default()
                    },
                },
                &deps,
            )
            .await?;
            if !payload.failures.is_empty() {
                return Err(AppError::InvalidData(format!(
                    "multi-chain batch completed with {} failed work units; see {}",
                    payload.failures.len(),
                    output_dir.join("failures.json").display()
                )));
            }
            Ok(())
        }),
        #[cfg(feature = "export-snapshot")]
        Command::ExportSnapshot(args) => {
            let block_range = SnapshotBlockRange::new(args.start_block, args.end_block)?
                .validate_for_chain(&args.chain)?;
            let mut conn = connect_postgres_from_constants()?;
            export_chain_snapshot_to_parquet(
                &mut conn,
                &args.chain,
                std::path::Path::new(&args.output),
                args.fetch_size,
                block_range,
            )?;
            Ok(())
        }
        #[cfg(not(feature = "export-snapshot"))]
        Command::ExportSnapshot(_) => Err(AppError::InvalidData(
            "export-snapshot requires building with --features export-snapshot".to_string(),
        )),
    }
}
