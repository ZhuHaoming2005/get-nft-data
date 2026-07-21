use clap::Parser;
use top_contract_analysis::cli::Cli;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> top_contract_analysis::Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.runtime_worker_threads()?)
        .enable_all()
        .build()?;
    runtime.block_on(cli.execute())
}
