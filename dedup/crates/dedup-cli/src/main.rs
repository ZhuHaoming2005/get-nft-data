mod pipeline;
mod progress;

use clap::{Parser, Subcommand};
use dedup_model::DedupError;
use progress::ProgressMode;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(
    name = "dedup",
    version,
    about = "Standalone multi-chain NFT deduplicator"
)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    diagnostic: bool,
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    #[arg(long, default_value_t = 1_000)]
    progress_interval_ms: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, Subcommand)]
enum Command {
    Preflight,
    BuildEntities,
    RunName,
    RunUri,
    RunMetadata,
    AuditMetadata,
    Report,
    All,
}

fn run() -> Result<(), DedupError> {
    let cli = Cli::parse();
    let context = pipeline::PipelineContext::load(
        cli.config,
        cli.diagnostic,
        cli.progress,
        Duration::from_millis(cli.progress_interval_ms),
    )?;
    match cli.command {
        Command::Preflight => context.track_stage("preflight", || context.preflight()),
        Command::BuildEntities => context.track_stage("entities", || context.build_entities()),
        Command::RunName => context.track_stage("name", || context.run_name()),
        Command::RunUri => context.track_stage("uri", || context.run_uri()),
        Command::RunMetadata => context.track_stage("metadata", || context.run_metadata()),
        Command::AuditMetadata => {
            context.track_stage("metadata_audit", || context.audit_metadata())
        }
        Command::Report => context.track_stage("report", || context.report()),
        Command::All => context.all(),
    }
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(error @ DedupError::Interrupted { .. }) => {
            eprintln!("{error}");
            std::process::exit(130);
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
