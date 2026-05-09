use std::path::PathBuf;

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use name_metadata_change_samples::{
    collect_samples_with_progress, SampleCollectionConfig, SampleProgressStage,
};

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
    max_recall_rows: usize,
    #[arg(long, default_value_t = 0)]
    max_seed_tokens: usize,
    #[arg(long, default_value_t = 0)]
    duckdb_threads: usize,
    #[arg(long, default_value = "80GB")]
    duckdb_memory_limit: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let output = args.output.clone();
    let multi = MultiProgress::new();
    let total_progress = multi.add(ProgressBar::new(0));
    total_progress.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] seeds [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
        )?
        .progress_chars("=>-"),
    );
    let seed_progress = multi.add(ProgressBar::new(0));
    seed_progress.set_style(
        ProgressStyle::with_template(
            "  current seed [{wide_bar:.magenta/blue}] {pos}/{len} {msg}",
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
            max_recall_rows: args.max_recall_rows,
            max_seed_tokens: args.max_seed_tokens,
            duckdb_threads: args.duckdb_threads,
            duckdb_memory_limit: args.duckdb_memory_limit,
        },
        |event| {
            total_progress.set_length(event.total_seeds as u64);
            let finished_seeds = if event.stage == SampleProgressStage::FinishedSeed {
                event.seed_index
            } else {
                event.seed_index.saturating_sub(1)
            };
            total_progress.set_position(finished_seeds as u64);
            total_progress.set_message(format!("seed {}/{}", event.seed_index, event.total_seeds));

            seed_progress.set_length(event.stage_count as u64);
            seed_progress.set_position(event.stage_index as u64);
            seed_progress.set_message(progress_message(event.stage, event.candidate_count));
        },
    )?;

    let candidate_count: usize = report
        .seed_reports
        .iter()
        .map(|seed| seed.name.matches.len() + seed.metadata.matches.len())
        .sum();
    seed_progress.finish_and_clear();
    total_progress.finish_with_message("done");
    println!(
        "wrote {} seed reports and {} name/metadata candidate groups to {}",
        report.seed_reports.len(),
        candidate_count,
        output.display()
    );
    Ok(())
}

fn progress_message(stage: SampleProgressStage, candidate_count: Option<usize>) -> String {
    let label = match stage {
        SampleProgressStage::ReadSeedRows => "read seed rows",
        SampleProgressStage::LoadNameCandidates => "load name candidates",
        SampleProgressStage::ScoreNameCandidates => "score name candidates",
        SampleProgressStage::PrepareMetadataQuery => "prepare metadata query",
        SampleProgressStage::CollectMetadataCandidates => "collect metadata candidates",
        SampleProgressStage::ScoreMetadataPrefilter => "score metadata prefilter",
        SampleProgressStage::LoadOverlappingMetadata => "load overlapping metadata",
        SampleProgressStage::ScoreOverlappingMetadata => "score overlapping metadata",
        SampleProgressStage::BuildReport => "build report",
        SampleProgressStage::FinishedSeed => "finish seed",
    };
    match candidate_count {
        Some(count) => format!("{label} candidates={count}"),
        None => label.to_string(),
    }
}
