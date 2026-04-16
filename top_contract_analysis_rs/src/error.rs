use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
}
