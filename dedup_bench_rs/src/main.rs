use std::path::PathBuf;

use clap::{Parser, Subcommand};

use dedup_bench_rs::{run_benchmark, BenchmarkConfig};

#[derive(Parser)]
#[command(name = "dedup_bench_rs")]
#[command(about = "Single-NFT name/metadata dedup benchmark")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        chain: String,
        #[arg(long, default_value = "")]
        contract_address: String,
        #[arg(long, default_value = "")]
        token_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        metadata_file: PathBuf,
        #[arg(long)]
        feature_db: PathBuf,
        #[arg(long)]
        feature_parquet: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 3)]
        repeat: usize,
        #[arg(long, default_value_t = 32)]
        algorithm_threads: usize,
    },
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Run {
            chain,
            contract_address,
            token_id,
            name,
            metadata_file,
            feature_db,
            feature_parquet,
            output,
            repeat,
            algorithm_threads,
        } => run_benchmark(&BenchmarkConfig {
            chain,
            contract_address,
            token_id,
            name,
            metadata_file,
            feature_db,
            feature_parquet,
            output,
            repeat,
            algorithm_threads,
        }),
    };

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
