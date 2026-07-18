//! Lossless candidate filtering followed by exact Jaro-Winkler verification.

mod candidate_bounds;
mod engine;

pub use candidate_bounds::*;
pub use engine::*;
