use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum AnalysisError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("seed manifest error: {0}")]
    Seed(String),
    #[error("input error: {0}")]
    Input(String),
    #[error("platform error: {0}")]
    Platform(String),
    #[error("memory budget exceeded: required {required} bytes, limit {limit} bytes")]
    MemoryBudget { required: u64, limit: u64 },
    #[error("identifier capacity exceeded for {kind}: {count}")]
    IdCapacity { kind: &'static str, count: u64 },
    #[error("state transition rejected: {0}")]
    State(String),
    #[error("provider error: {0}")]
    Provider(String),
    #[error("provider request failed: {message}")]
    ApiRequest {
        message: String,
        retryable: bool,
        retry_after_ms: Option<u64>,
    },
    #[error("analysis error: {0}")]
    Analysis(String),
    #[error("artifact error at {}: {message}", path.display())]
    Artifact { path: PathBuf, message: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

pub type Result<T> = std::result::Result<T, AnalysisError>;

impl AnalysisError {
    pub const fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Http(_)
                | Self::Json(_)
                | Self::ApiRequest {
                    retryable: true,
                    ..
                }
        )
    }

    pub const fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Self::ApiRequest { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }
}
