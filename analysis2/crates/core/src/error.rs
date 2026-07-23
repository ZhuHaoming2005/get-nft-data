use thiserror::Error;

/// Top-level error for the analysis2 pipeline.
#[derive(Debug, Error)]
pub enum Analysis2Error {
    #[error("invalid input: {0}")]
    Invalid(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("parquet error: {0}")]
    Parquet(String),

    #[error("http error: {0}")]
    Http(String),

    #[error("cancelled")]
    Cancelled,
}

impl Analysis2Error {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    pub fn parquet(message: impl Into<String>) -> Self {
        Self::Parquet(message.into())
    }

    pub fn http(message: impl Into<String>) -> Self {
        Self::Http(message.into())
    }
}

impl From<String> for Analysis2Error {
    fn from(value: String) -> Self {
        Self::Invalid(value)
    }
}

impl From<&str> for Analysis2Error {
    fn from(value: &str) -> Self {
        Self::Invalid(value.to_owned())
    }
}
