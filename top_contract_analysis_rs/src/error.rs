use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("http error: {0}")]
    Http(String),
    #[error("duckdb error: {0}")]
    DuckDb(String),
    #[error("postgres error: {0}")]
    Postgres(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("json error: {0}")]
    Json(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("interrupted: {0}")]
    Interrupted(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

impl From<duckdb::Error> for AppError {
    fn from(value: duckdb::Error) -> Self {
        Self::DuckDb(value.to_string())
    }
}

#[cfg(feature = "export-snapshot")]
impl From<postgres::Error> for AppError {
    fn from(value: postgres::Error) -> Self {
        Self::Postgres(value.to_string())
    }
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value.to_string())
    }
}

impl From<reqwest::Error> for AppError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value.to_string())
    }
}
