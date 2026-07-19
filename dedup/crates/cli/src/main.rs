mod pipeline;
mod progress;
mod report;

use clap::{Parser, Subcommand};
use dedup_core::DedupError;
use pipeline::RunConfig;
use progress::{ProgressMode, ProgressReporter};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "dedup", version, about = "In-memory NFT deduplicator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
struct CommonArgs {
    #[arg(long = "input", required = true)]
    inputs: Vec<PathBuf>,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, value_delimiter = ',')]
    chains: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    evm_chains: Vec<String>,
    #[arg(long, default_value_t = 95.0)]
    name_threshold: f64,
    #[arg(long, default_value_t = 0.6)]
    metadata_threshold: f64,
    #[arg(long, default_value_t = 8)]
    metadata_anchors: usize,
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    #[arg(long, default_value_t = 1_000)]
    progress_interval_ms: u64,
}

#[derive(Debug, Subcommand)]
enum Command {
    All(CommonArgs),
    RunName(CommonArgs),
    RunUri(CommonArgs),
    RunMetadata(CommonArgs),
}

fn main() {
    if let Err(error) = run() {
        match error {
            DedupError::Interrupted => {
                eprintln!("interrupted");
                std::process::exit(130);
            }
            other => {
                eprintln!("{other}");
                std::process::exit(1);
            }
        }
    }
}

fn run() -> Result<(), DedupError> {
    let cli = Cli::parse();
    let (args, run_name, run_uri, run_metadata) = match cli.command {
        Command::All(args) => (args, true, true, true),
        Command::RunName(args) => (args, true, false, false),
        Command::RunUri(args) => (args, false, true, false),
        Command::RunMetadata(args) => (args, false, false, true),
    };
    let progress_mode = args.progress;
    let progress_interval_ms = args.progress_interval_ms;
    let config = RunConfig {
        inputs: args.inputs,
        output_dir: args.output_dir,
        chains: args.chains,
        evm_chains: args.evm_chains,
        name_threshold: args.name_threshold,
        metadata_threshold: args.metadata_threshold,
        metadata_anchors: args.metadata_anchors,
        run_name,
        run_uri,
        run_metadata,
    };

    let mut reporter = ProgressReporter::start(progress_mode, progress_interval_ms);
    let cancel = reporter.cancel_handle();
    let _ = ctrlc::set_handler(move || {
        cancel.request_cancel();
    });

    let result = pipeline::run(config, &reporter);
    reporter.finish();
    result
}
