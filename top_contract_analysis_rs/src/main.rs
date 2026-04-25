use std::sync::Arc;

use clap::Parser;
use postgres::{Client, NoTls};
use tokio::runtime::Runtime;
use top_contract_analysis_rs::analysis::{
    analyze_seed_contract, read_seed_addresses, run_batch, AnalysisDeps, AnalyzeRequest,
    BatchRequest, RealApi,
};
use top_contract_analysis_rs::cli::{Command, TopContractAnalysisCli};
use top_contract_analysis_rs::config::postgres_connection_config;
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::progress::{
    create_batch_progress_reporter, create_single_seed_progress_reporter,
    NoopBatchProgressReporter, NoopProgressReporter,
};
use top_contract_analysis_rs::reporting::{write_batch_summary_outputs, write_default_outputs};
use top_contract_analysis_rs::store::{
    export_chain_snapshot_to_parquet, ContractSignalCache, DuckDbFeatureStore,
};

fn connect_postgres_from_constants() -> Result<Client, AppError> {
    let config = postgres_connection_config();
    Client::connect(&config, NoTls).map_err(AppError::from)
}

fn main() -> Result<(), AppError> {
    let command = TopContractAnalysisCli::parse();
    match command.command {
        Command::Analyze(args) => Runtime::new()?.block_on(async move {
            let feature_store = DuckDbFeatureStore::new(&args.feature_db)?;
            if !args.feature_parquet.trim().is_empty() {
                feature_store
                    .load_parquet_dataset_if_chain_missing(&args.chain, &args.feature_parquet)?;
            }
            let api = RealApi::new(
                args.timeout,
                args.api_max_concurrency,
                args.contract_max_concurrency,
                args.sale_metric_max_concurrency,
            )?;
            let deps = AnalysisDeps {
                api: Arc::new(api),
                feature_store: Box::new(feature_store),
                signal_cache: Some(Box::new(ContractSignalCache::new(&args.signal_cache_db)?)),
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
                    sale_metric_max_concurrency: args.sale_metric_max_concurrency,
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
            let feature_store = DuckDbFeatureStore::new(&args.feature_db)?;
            if !args.feature_parquet.trim().is_empty() {
                feature_store
                    .load_parquet_dataset_if_chain_missing(&args.chain, &args.feature_parquet)?;
            }
            let api = RealApi::new(args.timeout, 8, 4, 4)?;
            let deps = AnalysisDeps {
                api: Arc::new(api),
                feature_store: Box::new(feature_store),
                signal_cache: Some(Box::new(ContractSignalCache::new(&args.signal_cache_db)?)),
                progress: Arc::new(NoopProgressReporter),
                batch_progress: create_batch_progress_reporter(&seed_addresses, args.workers),
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
                    max_tokens_per_contract: args.max_tokens_per_contract,
                    max_recall_rows: args.max_recall_rows,
                    workers: args.workers,
                    ..BatchRequest::default()
                },
                &deps,
            )
            .await?;
            write_batch_summary_outputs(&payload, &output_dir)?;
            Ok(())
        }),
        Command::ExportSnapshot(args) => {
            let mut conn = connect_postgres_from_constants()?;
            export_chain_snapshot_to_parquet(
                &mut conn,
                &args.chain,
                std::path::Path::new(&args.output),
                args.fetch_size,
                args.keep_metadata_json,
            )?;
            Ok(())
        }
    }
}
