use std::path::PathBuf;

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use name_metadata_change_samples::{collect_samples_with_progress, SampleCollectionConfig};

#[derive(Debug, Parser)]
#[command(about = "Collect local name/metadata duplicate samples for seed contracts")]
struct Args {
    #[arg(long, default_value = "ethereum")]
    chain: String,
    #[arg(long)]
    feature_db: PathBuf,
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 95.0)]
    name_threshold: f64,
    #[arg(long, default_value_t = 0.6)]
    metadata_threshold: f64,
    #[arg(long, default_value_t = 0)]
    max_tokens_per_contract: usize,
    #[arg(long, default_value_t = 0)]
    max_recall_rows: usize,
    #[arg(long, default_value_t = 0)]
    max_seed_tokens: usize,
    #[arg(long, default_value_t = 1)]
    workers: usize,
    #[arg(long, default_value_t = 0)]
    duckdb_threads: usize,
    #[arg(long, default_value = "80GB")]
    duckdb_memory_limit: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let output = args.output.clone();
    let progress = ProgressBar::new(0);
    progress.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
        )?
        .progress_chars("=>-"),
    );

    let report = collect_samples_with_progress(
        SampleCollectionConfig {
            chain: args.chain,
            feature_db: args.feature_db,
            input: args.input,
            output: args.output,
            name_threshold: args.name_threshold,
            metadata_threshold: args.metadata_threshold,
            max_tokens_per_contract: args.max_tokens_per_contract,
            max_recall_rows: args.max_recall_rows,
            max_seed_tokens: args.max_seed_tokens,
            workers: args.workers,
            duckdb_threads: args.duckdb_threads,
            duckdb_memory_limit: args.duckdb_memory_limit,
        },
        |event| {
            progress.set_length(event.total as u64);
            progress.set_position(event.completed as u64);
            progress.set_message(format!(
                "{} candidates={}",
                event.contract_address, event.candidate_count
            ));
        },
    )?;

    let candidate_count: usize = report
        .seed_reports
        .iter()
        .map(|seed| seed.candidate_reports.len())
        .sum();
    progress.finish_with_message("done");
    println!(
        "wrote {} seed reports and {} name/metadata candidate groups to {}",
        report.seed_reports.len(),
        candidate_count,
        output.display()
    );
    Ok(())
}
