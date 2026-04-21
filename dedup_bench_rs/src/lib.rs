pub mod algorithms;
pub mod benchmark;
pub mod error;
pub mod report;
pub mod sample;
pub mod store;

pub use benchmark::{run_benchmark, BenchmarkConfig};

#[cfg(target_os = "windows")]
#[link(name = "Rstrtmgr")]
unsafe extern "system" {}
