use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorContext {
    pub stage: &'static str,
    pub partition: Option<u64>,
    pub stable_object_id: Option<u64>,
}

impl ErrorContext {
    pub const fn stage(stage: &'static str) -> Self {
        Self {
            stage,
            partition: None,
            stable_object_id: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum DedupError {
    #[error("invalid input in {context:?}: {message}")]
    InvalidInput {
        context: ErrorContext,
        message: String,
    },
    #[error("schema mismatch in {context:?}: {message}")]
    SchemaMismatch {
        context: ErrorContext,
        message: String,
    },
    #[error("snapshot conflict in {context:?}: {message}")]
    SnapshotConflict {
        context: ErrorContext,
        message: String,
    },
    #[error("invalid metadata in {context:?}: {message}")]
    InvalidMetadata {
        context: ErrorContext,
        message: String,
    },
    #[error("resource budget exceeded in {context:?}: requested {requested} bytes")]
    ResourceBudgetExceeded {
        context: ErrorContext,
        requested: u64,
    },
    #[error("work budget exhausted in {context:?}: {counter} reached {limit}")]
    BudgetExhausted {
        context: ErrorContext,
        counter: &'static str,
        limit: u64,
    },
    #[error("artifact mismatch in {context:?}: {message}")]
    ArtifactMismatch {
        context: ErrorContext,
        message: String,
    },
    #[error("metadata quality gate failed: recall {recall_ppm} ppm is below {required_ppm} ppm")]
    QualityGateFailed { recall_ppm: u32, required_ppm: u32 },
    #[error("metadata audit has {actual} positives, requires {required}")]
    InsufficientPositives { actual: u64, required: u64 },
    #[error("platform capability missing: {capability}")]
    PlatformCapabilityMissing { capability: String },
    #[error("invariant violation in {context:?}: {message}")]
    InvariantViolation {
        context: ErrorContext,
        message: String,
    },
    #[error("integer overflow while updating {counter}")]
    CounterOverflow { counter: &'static str },
    #[error("controlled shutdown requested while running {stage}")]
    Interrupted { stage: &'static str },
    #[error("I/O operation failed: {0}")]
    Io(#[from] std::io::Error),
}
