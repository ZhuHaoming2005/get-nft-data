use analysis2_cli::pipeline::{run as run_pipeline, run_dedup, RunConfig, RunDedupConfig};
use analysis2_cli::progress::{ProgressMode, ProgressReporter};
use analysis2_core::{
    select_seeds, write_seed_outputs, Analysis2Error, ApiKeys, PaperConfig, ProgressObserver,
    SelectSeedsOptions,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "analysis2",
    version,
    about = "Experimental in-memory NFT analysis pipeline"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Shared flags for `run` and `run-dedup`.
#[derive(Debug, Parser)]
struct RunArgs {
    /// Input Parquet snapshot (repeatable).
    #[arg(long = "input", required = true)]
    inputs: Vec<PathBuf>,

    /// Seed manifest JSON path.
    #[arg(long)]
    seeds: PathBuf,

    /// Output directory for reports.
    #[arg(long)]
    output_dir: PathBuf,

    /// Chains to include (comma-separated).
    #[arg(long, value_delimiter = ',')]
    chains: Vec<String>,

    /// EVM chains among `--chains` (comma-separated).
    #[arg(long, value_delimiter = ',')]
    evm_chains: Vec<String>,

    /// Name similarity threshold (Jaro-Winkler), default 0.98.
    #[arg(long, default_value_t = 0.98)]
    name_threshold: f64,

    /// Metadata BM25 threshold, default 0.6.
    #[arg(long, default_value_t = 0.6)]
    metadata_threshold: f64,

    /// Metadata anchors per contract, default 8.
    #[arg(long, default_value_t = 8)]
    metadata_anchors: usize,

    /// Alchemy API key (optional; missing → not_requested for dependent evidence).
    #[arg(long)]
    alchemy_api_key: Option<String>,

    /// Etherscan API key (optional).
    #[arg(long)]
    etherscan_api_key: Option<String>,

    /// Helius API key (optional).
    #[arg(long)]
    helius_api_key: Option<String>,

    /// OpenSea API key (optional).
    #[arg(long)]
    opensea_api_key: Option<String>,

    /// Rayon thread pool size.
    #[arg(long)]
    rayon_threads: Option<usize>,

    /// Per-provider HTTP concurrency (Alchemy / OpenSea / Helius / Etherscan each
    /// get an independent pool of this size). Saturating Alchemy does not block
    /// other providers. Keep modest: each candidate fans out to many nested RPCs.
    /// Default 12 avoids mass Alchemy timeouts.
    #[arg(long, default_value_t = 12)]
    http_concurrency: usize,

    /// Path for durable dedup cache (default: `<output-dir>/intermediate/dedup_cache.json`).
    /// Written after dedup on `run`; used when `--reuse-dedup` is set.
    #[arg(long)]
    dedup_cache: Option<PathBuf>,

    /// Skip URI/Name/Metadata queries; rematerialize hits from `--dedup-cache`
    /// (or `<output-dir>/intermediate/dedup_cache.json`). Still loads Parquet identity for
    /// enrich/analyze. Params/seeds must match the cache.
    #[arg(long, default_value_t = false)]
    reuse_dedup: bool,

    /// Path for durable evidence cache (default: `<output-dir>/intermediate/evidence_cache.json`).
    /// Written after enrich on `run`; used when `--reuse-evidence` is set.
    #[arg(long)]
    evidence_cache: Option<PathBuf>,

    /// Reuse enrich evidence from `--evidence-cache` (or default path). Only
    /// HTTP-fetches candidates missing from the cache. Seeds, pagination
    /// limits, and API-key presence must match the cache.
    #[arg(long, default_value_t = false)]
    reuse_evidence: bool,

    /// Progress reporter mode.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
}

#[derive(Debug, Parser)]
struct SelectSeedsArgs {
    /// Output directory for `seeds.json` and `seeds.audit.json`.
    #[arg(long)]
    output_dir: PathBuf,

    /// Chains to select seeds for (comma-separated).
    #[arg(long, value_delimiter = ',')]
    chains: Vec<String>,

    /// Top-N seeds per chain (default 25).
    #[arg(long, default_value_t = 25)]
    seeds_per_chain: usize,

    /// OpenSea API key (required for EVM ranking in later tasks).
    #[arg(long)]
    opensea_api_key: Option<String>,

    /// Helius API key (optional; Solana collection resolve).
    #[arg(long)]
    helius_api_key: Option<String>,

    /// Per-provider HTTP concurrency (independent pools for each API provider).
    #[arg(long, default_value_t = 32)]
    http_concurrency: usize,

    /// Progress reporter mode.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Build per-chain top-N seed list + audit JSON.
    SelectSeeds(SelectSeedsArgs),
    /// End-to-end: load → dedup → enrich → analyze → reports.
    Run(RunArgs),
    /// Debug path: load + dedup + hit/candidate reports only.
    RunDedup(RunArgs),
}

fn main() {
    if let Err(error) = run() {
        match error {
            Analysis2Error::Cancelled => {
                eprintln!("cancelled");
                std::process::exit(130);
            }
            other => {
                eprintln!("{other}");
                std::process::exit(1);
            }
        }
    }
}

fn with_progress<F>(mode: ProgressMode, f: F) -> Result<(), Analysis2Error>
where
    F: FnOnce(&ProgressReporter) -> Result<(), Analysis2Error>,
{
    let reporter = ProgressReporter::start(mode, 500);
    let result = f(&reporter);
    reporter.finish();
    result
}

fn run() -> Result<(), Analysis2Error> {
    let cli = Cli::parse();
    match cli.command {
        Command::SelectSeeds(args) => with_progress(args.progress, |progress| {
            let chains = if args.chains.is_empty() {
                SelectSeedsOptions::default().chains
            } else {
                args.chains.clone()
            };
            progress.begin_phase("select-seeds", Some(chains.len() as u64));
            let (seeds, audit) = select_seeds(&SelectSeedsOptions {
                chains: chains.clone(),
                seeds_per_chain: args.seeds_per_chain,
                opensea_api_key: args.opensea_api_key.clone(),
                helius_api_key: args.helius_api_key.clone(),
                http_concurrency: args.http_concurrency,
                ..SelectSeedsOptions::default()
            })?;
            write_seed_outputs(&args.output_dir, &seeds, &audit)?;
            progress.add_completed(chains.len() as u64);
            eprintln!(
                "select-seeds: wrote {} seeds to {}",
                seeds.len(),
                args.output_dir.join("seeds.json").display()
            );
            Ok(())
        }),
        Command::Run(args) => with_progress(args.progress, |progress| {
            run_pipeline(
                &RunConfig {
                    inputs: args.inputs,
                    seeds: args.seeds,
                    output_dir: args.output_dir,
                    chains: args.chains,
                    evm_chains: args.evm_chains,
                    name_threshold: args.name_threshold,
                    metadata_threshold: args.metadata_threshold,
                    metadata_anchors: args.metadata_anchors,
                    rayon_threads: args.rayon_threads,
                    api_keys: ApiKeys {
                        alchemy: args.alchemy_api_key,
                        etherscan: args.etherscan_api_key,
                        helius: args.helius_api_key,
                        opensea: args.opensea_api_key,
                    },
                    http_concurrency: args.http_concurrency,
                    paper: PaperConfig::default(),
                    enrich_override: None,
                    dedup_cache_path: args.dedup_cache,
                    reuse_dedup: args.reuse_dedup,
                    evidence_cache_path: args.evidence_cache,
                    reuse_evidence: args.reuse_evidence,
                },
                progress,
            )
        }),
        Command::RunDedup(args) => with_progress(args.progress, |progress| {
            run_dedup(
                &RunDedupConfig {
                    inputs: args.inputs,
                    seeds: args.seeds,
                    output_dir: args.output_dir,
                    chains: args.chains,
                    evm_chains: args.evm_chains,
                    name_threshold: args.name_threshold,
                    metadata_threshold: args.metadata_threshold,
                    metadata_anchors: args.metadata_anchors,
                    rayon_threads: args.rayon_threads,
                },
                progress,
            )
        }),
    }
}
