use std::path::PathBuf;

use clap::Parser;
use name_uri_analysis_rs::analysis::{run_analysis, AnalysisOptions};

/// Resolve the worker thread count shared by DuckDB and Rayon.
///
/// Defaults to SMT (all logical cores reported by `available_parallelism`).
/// `--physical-cores` is the hardware-aware opt-in: when set it pins every
/// stage to N threads, which on a 32-physical-core SMT host means passing
/// `--physical-cores 32` to use physical cores only for the compute-bound
/// DuckDB hash joins and Jaro-Winkler scoring. Precedence:
/// `--physical-cores` > `--threads` > SMT default.
///
/// When an explicit count is given, the global Rayon pool is pinned to it too,
/// so any parallel work that runs outside the name-analysis thread pool still
/// respects the budget instead of filling every logical core.
fn resolve_threads(threads: usize, physical_cores: usize) -> usize {
    if physical_cores > 0 {
        pin_global_rayon_pool(physical_cores);
        physical_cores
    } else if threads > 0 {
        pin_global_rayon_pool(threads);
        threads
    } else {
        std::thread::available_parallelism()
            .map(|threads| threads.get())
            .unwrap_or(1)
    }
}

fn pin_global_rayon_pool(threads: usize) {
    // Best-effort: if the global pool was already initialized this errors, in
    // which case Rayon keeps its default. Called once at startup before any
    // `par_iter`, so it normally succeeds.
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global();
}

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

    /// Worker thread count shared by DuckDB and Rayon.
    /// 0 = auto = SMT (all logical cores). On a 32-physical-core SMT host this
    /// defaults to 64; pass `--physical-cores 32` to pin to physical cores for
    /// the compute-bound DuckDB/name stages.
    #[arg(long, default_value_t = 0)]
    threads: usize,

    /// Physical core count. When set, overrides `--threads` and pins DuckDB +
    /// Rayon to N threads (recommended for compute-bound work on this host).
    #[arg(long, default_value_t = 0)]
    physical_cores: usize,

    #[arg(
        long,
        default_value = "auto",
        help = "Rust name-analysis adaptive batching budget"
    )]
    memory_limit: String,

    #[arg(
        long,
        help = "Optional Rust name-analysis budget inside --memory-limit"
    )]
    analysis_memory_limit: Option<String>,

    /// DuckDB memory limit. "auto" derives ~75% of available memory, leaving
    /// headroom for the Rust analysis structures; otherwise a size like
    /// "200GB". DuckDB and Rust share the process address space.
    #[arg(long, default_value = "auto")]
    duckdb_memory_limit: String,

    /// DuckDB temp directory for hash-join spilling. Defaults to a subdirectory
    /// of the system temp dir; point at a fast local NVMe for large inputs.
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
    let threads = resolve_threads(args.threads, args.physical_cores);
    let temp_directory = args
        .temp_directory
        .or_else(|| Some(std::env::temp_dir().join("name_uri_analysis_rs_duckdb")));
    let report = run_analysis(AnalysisOptions {
        database_path: args.database,
        parquet_inputs: args.parquet_inputs,
        output_dir: args.output_dir.clone(),
        thresholds: args.thresholds,
        threads,
        memory_limit: args.memory_limit,
        analysis_memory_limit: args.analysis_memory_limit,
        duckdb_memory_limit: args.duckdb_memory_limit,
        temp_directory,
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
