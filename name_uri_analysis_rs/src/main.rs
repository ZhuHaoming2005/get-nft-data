use std::path::PathBuf;

use clap::Parser;
use name_uri_analysis_rs::analysis::{run_analysis, AnalysisOptions};

const DEFAULT_THREADS: usize = 96;

#[derive(Debug, Parser)]
#[command(version, about = "Rust + DuckDB NFT name/URI duplicate analysis")]
struct Args {
    #[arg(long = "parquet", required = true)]
    parquet_inputs: Vec<PathBuf>,

    #[arg(long, default_value = ":memory:", hide = true)]
    database: PathBuf,

    #[arg(long, default_value = "name_uri_analysis_output")]
    output_dir: PathBuf,

    #[arg(long, value_delimiter = ',', default_value = "95")]
    thresholds: Vec<f64>,

    #[arg(long, default_value_t = DEFAULT_THREADS)]
    threads: usize,

    #[arg(
        long,
        default_value = "auto",
        help = "Rust name-analysis adaptive batching budget; DuckDB is left unrestricted"
    )]
    memory_limit: String,

    #[arg(
        long,
        help = "Optional Rust name-analysis budget inside --memory-limit; DuckDB is left unrestricted"
    )]
    analysis_memory_limit: Option<String>,

    #[arg(long)]
    temp_directory: Option<PathBuf>,

    #[arg(long, hide = true)]
    persist_prepared: bool,

    #[arg(long, hide = true)]
    reuse_prepared: bool,

    #[arg(long, help = "Disable terminal progress bars")]
    no_progress: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let report = run_analysis(AnalysisOptions {
        database_path: args.database,
        parquet_inputs: args.parquet_inputs,
        output_dir: args.output_dir.clone(),
        thresholds: args.thresholds,
        threads: args.threads,
        memory_limit: args.memory_limit,
        analysis_memory_limit: args.analysis_memory_limit,
        temp_directory: args.temp_directory,
        progress: !args.no_progress,
        persist_prepared: args.persist_prepared,
        reuse_prepared: args.reuse_prepared,
    })?;
    println!(
        "wrote {} summary rows to {}",
        report.summary_rows.len(),
        args.output_dir.display()
    );
    Ok(())
}
