pub mod entity;
pub mod error;
pub mod metadata;
pub mod name;
pub mod parquet;
pub mod progress;
pub mod scope;
pub mod stats;
pub mod uri;

pub use entity::{Dimension, EntityStore, ScopeKind};
pub use error::DedupError;
pub use metadata::{run_metadata, MetadataRunResult, PrefilterConfig, PrefilterStats};
pub use name::run_name;
pub use parquet::load_entities;
pub use progress::{EwmaEta, NoopProgress, ProgressObserver};
pub use stats::SummaryAccumulator;
pub use uri::run_uri;
