use std::sync::Arc;

use clap::Parser;
#[cfg(feature = "export-snapshot")]
use postgres::{Client, NoTls};
use tokio::runtime::Runtime;
use top_contract_analysis_rs::analysis::{
    analyze_seed_contract, read_seed_addresses, run_batch, AnalysisDeps, AnalyzeRequest,
    BatchRequest, RealApi,
};
use top_contract_analysis_rs::cli::{Command, TopContractAnalysisCli};
#[cfg(feature = "export-snapshot")]
use top_contract_analysis_rs::config::postgres_connection_config;
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::progress::{
    create_batch_progress_reporter, create_single_seed_progress_reporter,
    NoopBatchProgressReporter, NoopProgressReporter,
};
use top_contract_analysis_rs::reporting::{write_batch_summary_outputs, write_default_outputs};
#[cfg(feature = "export-snapshot")]
use top_contract_analysis_rs::store::export_chain_snapshot_to_parquet;
use top_contract_analysis_rs::store::{
    ContractSignalCache, DuckDbFeatureStore, DuckDbResourceOptions,
};

#[cfg(feature = "export-snapshot")]
fn connect_postgres_from_constants() -> Result<Client, AppError> {
    let config = postgres_connection_config();
    Client::connect(&config, NoTls).map_err(AppError::from)
}

fn main() -> Result<(), AppError> {
    let command = TopContractAnalysisCli::parse();
    match command.command {
        Command::Analyze(args) => Runtime::new()?.block_on(async move {
            let duckdb_options =
                DuckDbResourceOptions::from_cli(args.duckdb_threads, &args.duckdb_memory_limit)?;
            let feature_store =
                DuckDbFeatureStore::new_with_options(&args.feature_db, duckdb_options)?;
            if !args.feature_parquet.trim().is_empty() {
                feature_store
                    .load_parquet_dataset_if_chain_missing(&args.chain, &args.feature_parquet)?;
            }
            let api = RealApi::new(args.timeout, args.api_max_concurrency)?;
            let deps = AnalysisDeps {
                api: Arc::new(api),
                feature_store: Arc::new(feature_store),
                signal_cache: Some(Arc::new(ContractSignalCache::new(&args.signal_cache_db)?)),
                progress: create_single_seed_progress_reporter(&args.seed_contract_address),
                batch_progress: Arc::new(NoopBatchProgressReporter),
            };
            let payload = analyze_seed_contract(
                AnalyzeRequest {
                    chain: args.chain,
                    seed_contract_address: args.seed_contract_address.to_lowercase(),
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
                    api_max_concurrency: args.api_max_concurrency,
                    contract_max_concurrency: args.contract_max_concurrency,
                    max_tokens_per_contract: args.max_tokens_per_contract,
                    max_recall_rows: args.max_recall_rows,
                },
                &deps,
            )
            .await?;
            write_default_outputs(&payload, &args.output)?;
            Ok(())
        }),
        Command::Batch(args) => Runtime::new()?.block_on(async move {
            let seed_addresses = read_seed_addresses(std::path::Path::new(&args.seed_file))?;
            let duckdb_options =
                DuckDbResourceOptions::from_cli(args.duckdb_threads, &args.duckdb_memory_limit)?;
            let feature_store =
                DuckDbFeatureStore::new_with_options(&args.feature_db, duckdb_options)?;
            if !args.feature_parquet.trim().is_empty() {
                feature_store
                    .load_parquet_dataset_if_chain_missing(&args.chain, &args.feature_parquet)?;
            }
            let api = RealApi::new(args.timeout, args.api_max_concurrency)?;
            let deps = AnalysisDeps {
                api: Arc::new(api),
                feature_store: Arc::new(feature_store),
                signal_cache: Some(Arc::new(ContractSignalCache::new(&args.signal_cache_db)?)),
                progress: Arc::new(NoopProgressReporter),
                batch_progress: create_batch_progress_reporter(
                    &seed_addresses,
                    args.seed_network_max_concurrency,
                ),
            };
            let output_dir = std::path::PathBuf::from(&args.output_dir);
            let payload = run_batch(
                BatchRequest {
                    chain: args.chain,
                    seed_file: std::path::PathBuf::from(args.seed_file),
                    output_dir: output_dir.clone(),
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
                    api_max_concurrency: args.api_max_concurrency,
                    seed_metadata_max_concurrency: args.seed_metadata_max_concurrency,
                    contract_max_concurrency: args.contract_max_concurrency,
                    seed_network_max_concurrency: args.seed_network_max_concurrency,
                    seed_cpu_max_concurrency: args.seed_cpu_max_concurrency,
                    max_tokens_per_contract: args.max_tokens_per_contract,
                    max_recall_rows: args.max_recall_rows,
                },
                &deps,
            )
            .await?;
            write_batch_summary_outputs(&payload, &output_dir)?;
            Ok(())
        }),
        #[cfg(feature = "export-snapshot")]
        Command::ExportSnapshot(args) => {
            let mut conn = connect_postgres_from_constants()?;
            export_chain_snapshot_to_parquet(
                &mut conn,
                &args.chain,
                std::path::Path::new(&args.output),
                args.fetch_size,
            )?;
            Ok(())
        }
        #[cfg(not(feature = "export-snapshot"))]
        Command::ExportSnapshot(_) => Err(AppError::InvalidData(
            "export-snapshot requires building with --features export-snapshot".to_string(),
        )),
    }
}
