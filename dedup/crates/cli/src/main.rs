mod pipeline;
mod progress;
mod report;
mod sample_images;

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
    #[arg(
        long,
        help = "Name similarity percentage; omission disables Name dedup"
    )]
    name_threshold: Option<f64>,
    #[arg(long, default_value_t = 0.6)]
    metadata_threshold: f64,
    #[arg(
        long,
        help = "Metadata anchors per contract; omission keeps every valid NFT metadata record"
    )]
    metadata_anchors: Option<usize>,
    #[arg(
        long,
        default_value_t = 0,
        help = "Explicitly enable bounded contract-pair sampling; Metadata samples require both compared tokens to have image URIs and download those images"
    )]
    sample_pairs: usize,
    #[arg(
        long,
        value_parser = parse_positive_usize,
        help = "Image-qualified Metadata candidates inspected for --sample-pairs; omission uses max(N*16, N+256) and does not enable sampling by itself"
    )]
    sample_candidate_limit: Option<usize>,
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    #[arg(long, default_value_t = 1_000)]
    progress_interval_ms: u64,
    #[arg(
        long,
        value_parser = parse_positive_usize,
        help = "Worker threads; omission uses Rayon's system default"
    )]
    threads: Option<usize>,
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
    let sample_candidate_limit =
        resolve_sample_candidate_limit(args.sample_pairs, args.sample_candidate_limit)?;
    if let Some(threads) = args.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .map_err(|error| {
                DedupError::Message(format!(
                    "failed to configure {threads} worker threads: {error}"
                ))
            })?;
    }
    let config = RunConfig {
        inputs: args.inputs,
        output_dir: args.output_dir,
        chains: args.chains,
        evm_chains: args.evm_chains,
        name_threshold: args.name_threshold,
        metadata_threshold: args.metadata_threshold,
        metadata_anchors: args.metadata_anchors,
        sample_pairs: args.sample_pairs,
        sample_candidate_limit,
        threads: rayon::current_num_threads(),
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

fn resolve_sample_candidate_limit(
    sample_pairs: usize,
    requested: Option<usize>,
) -> Result<usize, DedupError> {
    if sample_pairs == 0 {
        return if requested.is_some() {
            Err(DedupError::Message(
                "--sample-candidate-limit requires --sample-pairs greater than zero".to_owned(),
            ))
        } else {
            Ok(0)
        };
    }
    let automatic = sample_pairs
        .saturating_mul(16)
        .max(sample_pairs.saturating_add(256));
    let limit = requested.unwrap_or(automatic);
    if limit < sample_pairs {
        return Err(DedupError::Message(format!(
            "--sample-candidate-limit ({limit}) must be at least --sample-pairs ({sample_pairs})"
        )));
    }
    Ok(limit)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| "must be a positive integer".to_owned())?;
    if parsed == 0 {
        Err("must be greater than zero".to_owned())
    } else {
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_positive_usize, resolve_sample_candidate_limit};

    #[test]
    fn thread_count_must_be_positive() {
        assert_eq!(parse_positive_usize("64").unwrap(), 64);
        assert!(parse_positive_usize("0").is_err());
        assert!(parse_positive_usize("invalid").is_err());
    }

    #[test]
    fn metadata_candidate_limit_is_bounded_and_must_cover_the_target() {
        assert_eq!(resolve_sample_candidate_limit(0, None).unwrap(), 0);
        assert!(resolve_sample_candidate_limit(0, Some(10)).is_err());
        assert_eq!(resolve_sample_candidate_limit(10, None).unwrap(), 266);
        assert_eq!(resolve_sample_candidate_limit(10, Some(20)).unwrap(), 20);
        assert!(resolve_sample_candidate_limit(10, Some(9)).is_err());
    }
}
