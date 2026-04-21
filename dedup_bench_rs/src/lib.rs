mod algorithms;
mod benchmark;
mod decision_rules;
mod error;
mod report;
mod sample;
mod store;

pub use benchmark::{run_benchmark, BenchmarkConfig};

#[cfg(target_os = "windows")]
#[link(name = "Rstrtmgr")]
unsafe extern "system" {}
