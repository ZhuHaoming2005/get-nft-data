use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
#[cfg(feature = "export-snapshot")]
use postgres::{Client, NoTls};
use tokio::runtime::Runtime;
use top_contract_analysis_rs::analysis::multichain::parse_alchemy_networks;
use top_contract_analysis_rs::analysis::paper_stats::PaperStatsConfig;
use top_contract_analysis_rs::analysis::read_seed_contracts;
use top_contract_analysis_rs::analysis::{
    acquire_batch_output_lock, analyze_seed_contract, run_multichain_batch_with_lock, AnalysisDeps,
    AnalyzeRequest, HeliusApiConfig, MultiChainBatchRequest, RealApi,
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

/// Keep DuckDB query workers and Rayon scoring workers independent. An explicit
/// physical-core count caps DuckDB and intentionally pins Rayon to physical
/// cores, avoiding accidental SMT oversubscription on machines where it is set.
fn resolve_resource_threads(
    physical_cores: usize,
    duckdb_threads: usize,
    rayon_threads: usize,
) -> (usize, usize) {
    if physical_cores > 0 {
        (duckdb_threads.min(physical_cores), physical_cores)
    } else {
        (duckdb_threads, rayon_threads)
    }
}

fn configure_rayon_threads(rayon_threads: usize) -> Result<(), AppError> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(rayon_threads)
        .build_global()
        .map_err(|error| {
            AppError::InvalidData(format!(
                "failed to configure the global Rayon pool with {rayon_threads} workers: {error}"
            ))
        })
}

fn main() -> Result<(), AppError> {
    let command = TopContractAnalysisCli::parse();
    match command.command {
        Command::Analyze(args) => Runtime::new()?.block_on(async move {
            if !args.feature_parquet.trim().is_empty() {
                return Err(AppError::InvalidData(
                    "analyze does not import Parquet; run prepare-features first".to_string(),
                ));
            }
            let analyze_chain = args.chain.parse::<Chain>()?;
            let (duckdb_threads, rayon_threads) = resolve_resource_threads(
                args.physical_cores,
                args.duckdb_threads,
                args.rayon_threads,
            );
            configure_rayon_threads(rayon_threads)?;
            let duckdb_options = DuckDbResourceOptions::from_analysis_cli(
                duckdb_threads,
                &args.duckdb_memory_limit,
                &args.recall_index_memory_limit,
                args.duckdb_read_connections,
                &args.max_snapshot_bytes_per_seed,
                args.max_candidate_contracts_per_seed,
                args.max_selected_rows_per_seed,
            )?;
            eprintln!(
                "analyze: duckdb_threads={}, rayon_threads={}, duckdb_read_connections={}, duckdb_memory_limit={}, recall_index_memory_limit={}, feature_db={}, max_tokens_per_contract={}, max_candidate_contracts_per_seed={}, max_selected_rows_per_seed={}, max_snapshot_bytes_per_seed={}",
                duckdb_options.threads,
                rayon_threads,
                duckdb_options.read_connections,
                duckdb_options.memory_limit,
                args.recall_index_memory_limit,
                args.feature_db,
                args.max_tokens_per_contract,
                args.max_candidate_contracts_per_seed,
                args.max_selected_rows_per_seed,
                args.max_snapshot_bytes_per_seed,
            );
            let feature_store = DuckDbFeatureStore::open_read_only_with_options(
                &args.feature_db,
                duckdb_options,
            )?;
            feature_store.require_prepared_for_chains(&[analyze_chain])?;
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
                    matched_contract_max_concurrency: args.matched_contract_max_concurrency,
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
                        analysis_timestamp: args.paper_analysis_timestamp,
                    },
                },
                &deps,
            )
            .await?;
            write_default_outputs(&payload, &args.output)?;
            Ok(())
        }),
        Command::Batch(args) => Runtime::new()?.block_on(async move {
            if !args.feature_parquet.is_empty() {
                return Err(AppError::InvalidData(
                    "batch does not import Parquet; run prepare-features first".to_string(),
                ));
            }
            let output_dir = std::path::PathBuf::from(&args.output_dir);
            let output_lock = acquire_batch_output_lock(&output_dir)?;
            let seed_contracts = read_seed_contracts(std::path::Path::new(&args.seed_file))?;
            let seed_labels = seed_contracts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let (duckdb_threads, rayon_threads) = resolve_resource_threads(
                args.physical_cores,
                args.duckdb_threads,
                args.rayon_threads,
            );
            configure_rayon_threads(rayon_threads)?;
            let duckdb_options = DuckDbResourceOptions::from_analysis_cli(
                duckdb_threads,
                &args.duckdb_memory_limit,
                &args.recall_index_memory_limit,
                args.duckdb_read_connections,
                &args.max_snapshot_bytes_per_seed,
                args.max_candidate_contracts_per_seed,
                args.max_selected_rows_per_seed,
            )?;
            eprintln!(
                "batch: duckdb_threads={}, rayon_threads={}, duckdb_read_connections={}, duckdb_memory_limit={}, recall_index_memory_limit={}, feature_db={}, seed_cpu_max_concurrency={}, max_tokens_per_contract={}, max_candidate_contracts_per_seed={}, max_selected_rows_per_seed={}, max_snapshot_bytes_per_seed={}",
                duckdb_options.threads,
                rayon_threads,
                duckdb_options.read_connections,
                duckdb_options.memory_limit,
                args.recall_index_memory_limit,
                args.feature_db,
                args.seed_cpu_max_concurrency,
                args.max_tokens_per_contract,
                args.max_candidate_contracts_per_seed,
                args.max_selected_rows_per_seed,
                args.max_snapshot_bytes_per_seed,
            );
            let feature_store = DuckDbFeatureStore::open_read_only_with_options(
                &args.feature_db,
                duckdb_options,
            )?;
            feature_store.require_prepared_for_chains(&Chain::ALL)?;
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
                    matched_contract_max_concurrency: args.matched_contract_max_concurrency,
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
            let cancellation_requested = Arc::new(AtomicBool::new(false));
            let batch = run_multichain_batch_with_lock(
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
                    max_history_transactions_per_asset: args.max_history_transactions_per_asset,
                    max_history_transactions_per_collection: args
                        .max_history_transactions_per_collection,
                    max_helius_assets_per_collection: args.max_helius_assets_per_collection,
                    paper_stats_config: PaperStatsConfig {
                        min_cycle_size: args.paper_min_cycle_size,
                        min_path_length: args.paper_min_path_length,
                        center_fanout_threshold: args.paper_center_fanout_threshold,
                        concentration_top_pct: args.paper_concentration_top_pct,
                        analysis_timestamp: args.paper_analysis_timestamp,
                    },
                    refresh_scoped_cache: args.refresh_scoped_cache,
                    cancellation_requested: cancellation_requested.clone(),
                },
                &deps,
                output_lock,
            );
            tokio::pin!(batch);
            let payload = tokio::select! {
                result = &mut batch => result?,
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    cancellation_requested.store(true, Ordering::Release);
                    eprintln!(
                        "interrupt requested: stopping at a whole-seed recovery boundary; press Ctrl+C again for immediate exit"
                    );
                    tokio::select! {
                        result = &mut batch => match result {
                            Err(AppError::Interrupted(message)) => {
                                eprintln!("interrupted: {message}");
                                // Do not drop the runtime and wait indefinitely
                                // for uncancellable DuckDB spawn_blocking work.
                                // The batch has already synced its incomplete
                                // manifest and every report is atomically written.
                                std::process::exit(130);
                            }
                            result => result?,
                        },
                        second_signal = tokio::signal::ctrl_c() => {
                            second_signal?;
                            eprintln!("second interrupt received: exiting immediately");
                            std::process::exit(130);
                        }
                    }
                }
            };
            if !payload.failures.is_empty() {
                return Err(AppError::InvalidData(format!(
                    "multi-chain batch completed with {} failed work units; see {}",
                    payload.failures.len(),
                    output_dir.join("failures.json").display()
                )));
            }
            Ok(())
        }),
        Command::PrepareFeatures(args) => {
            if args.feature_db == ":memory:" && !args.allow_in_memory_feature_db {
                return Err(AppError::InvalidData(
                    "in-memory feature preparation requires --allow-in-memory-feature-db"
                        .to_string(),
                ));
            }
            let (duckdb_threads, rayon_threads) = resolve_resource_threads(
                args.physical_cores,
                args.duckdb_threads,
                args.rayon_threads,
            );
            configure_rayon_threads(rayon_threads)?;
            let duckdb_options =
                DuckDbResourceOptions::from_cli(duckdb_threads, &args.duckdb_memory_limit)?;
            eprintln!(
                "prepare-features: duckdb_threads={}, rayon_threads={}, memory_limit={}, feature_db={}, parquet_files={}, prepare_only={}, restart_prepare={}",
                duckdb_options.threads,
                rayon_threads,
                duckdb_options.memory_limit,
                args.feature_db,
                args.feature_parquet.len(),
                args.prepare_only,
                args.restart_prepare
            );
            let feature_store =
                DuckDbFeatureStore::new_with_options(&args.feature_db, duckdb_options)?;
            let chains = if args.prepare_only {
                if !args.feature_parquet.is_empty() {
                    return Err(AppError::InvalidData(
                        "--prepare-only cannot be combined with --feature-parquet".to_string(),
                    ));
                }
                feature_store.resume_authoritative_prepare()?
            } else {
                if args.feature_parquet.is_empty() {
                    return Err(AppError::InvalidData(
                        "prepare-features requires --feature-parquet unless --prepare-only is used"
                            .to_string(),
                    ));
                }
                feature_store.import_authoritative_parquet_snapshot(
                    &args.feature_parquet,
                    args.restart_prepare,
                )?
            };
            feature_store.prepare_recall_for_chains(&chains)?;
            Ok(())
        }
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

#[cfg(test)]
mod tests {
    use super::resolve_resource_threads;

    #[test]
    fn resource_threads_keep_duckdb_and_rayon_budgets_independent() {
        assert_eq!(resolve_resource_threads(0, 64, 96), (64, 96));
        assert_eq!(resolve_resource_threads(64, 96, 96), (64, 64));
        assert_eq!(resolve_resource_threads(128, 64, 96), (64, 128));
    }
}
