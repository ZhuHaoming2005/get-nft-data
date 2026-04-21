use thiserror::Error;

#[derive(Debug, Error)]
pub enum BenchError {
    #[error("duckdb error: {0}")]
    DuckDb(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("json error: {0}")]
    Json(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
}

impl From<duckdb::Error> for BenchError {
    fn from(value: duckdb::Error) -> Self {
        Self::DuckDb(value.to_string())
    }
}

impl From<std::io::Error> for BenchError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<serde_json::Error> for BenchError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value.to_string())
    }
}
