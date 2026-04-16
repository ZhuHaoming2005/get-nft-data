mod cli;
mod error;

use clap::Parser;

fn main() -> Result<(), error::AppError> {
    let command = cli::TopContractAnalysisCli::parse();
    match command.command {
        cli::Command::Analyze(args) => Err(error::AppError::NotImplemented(format!(
            "analyze {:?}",
            args.seed_contract_address
        ))),
        cli::Command::Batch(args) => Err(error::AppError::NotImplemented(format!(
            "batch {:?}",
            args.seed_file
        ))),
        cli::Command::ExportSnapshot(args) => Err(error::AppError::NotImplemented(format!(
            "export-snapshot {:?}",
            args.output
        ))),
    }
}
