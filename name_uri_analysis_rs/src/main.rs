use std::path::PathBuf;

use clap::Parser;
use name_uri_analysis_rs::analysis::{run_analysis, AnalysisOptions};

#[derive(Debug, Parser)]
#[command(version, about = "Rust + DuckDB NFT name/URI duplicate analysis")]
struct Args {
    #[arg(long = "parquet", required = true)]
    parquet_inputs: Vec<PathBuf>,

    #[arg(long, default_value = "name_uri_analysis.duckdb")]
    database: PathBuf,

    #[arg(long, default_value = "name_uri_analysis_output")]
    output_dir: PathBuf,

    #[arg(long, value_delimiter = ',', default_value = "90,95,98")]
    thresholds: Vec<f64>,

    #[arg(long, default_value_t = 32)]
    threads: usize,

    #[arg(
        long,
        default_value = "8GB",
        help = "Total memory budget; DuckDB and Rust analysis share this budget"
    )]
    memory_limit: String,

    #[arg(
        long,
        help = "Reserve part of --memory-limit for Rust name analysis; accepts sizes like 16GB or auto"
    )]
    analysis_memory_limit: Option<String>,

    #[arg(long)]
    temp_directory: Option<PathBuf>,
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
    })?;
    println!(
        "wrote {} summary rows to {}",
        report.summary_rows.len(),
        args.output_dir.display()
    );
    Ok(())
}
