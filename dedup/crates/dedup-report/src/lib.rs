//! Scope-owned hit de-duplication and deterministic statistics.

mod hit_sink;
mod metrics;

pub use hit_sink::*;
pub use metrics::*;
