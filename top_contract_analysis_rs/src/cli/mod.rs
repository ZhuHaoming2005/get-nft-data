use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
pub struct TopContractAnalysisCli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Analyze(AnalyzeArgs),
    Batch(BatchArgs),
    ExportSnapshot(ExportSnapshotArgs),
}

#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    #[arg(long, default_value = "ethereum")]
    pub chain: String,
    #[arg(long)]
    pub seed_contract_address: String,
    #[arg(long, default_value = "")]
    pub alchemy_api_key: String,
    #[arg(long, default_value = "")]
    pub feature_db: String,
}

#[derive(Args, Debug)]
pub struct BatchArgs {
    #[arg(long)]
    pub seed_file: String,
}

#[derive(Args, Debug)]
pub struct ExportSnapshotArgs {
    #[arg(long)]
    pub output: String,
}
