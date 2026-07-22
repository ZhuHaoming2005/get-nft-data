use analysis::cli::Cli;
use clap::Parser;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> analysis::Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.runtime_worker_threads()?)
        .enable_all()
        .build()?;
    runtime.block_on(cli.execute())
}
