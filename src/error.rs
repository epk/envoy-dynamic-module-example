use google_cloud_spanner::client::Error as SpannerError;
use std::time::Duration;
use thiserror::Error;

/// Error types for the Envoy Spanner extension
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Spanner client error: {0}")]
    Spanner(#[from] SpannerError),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Spanner operation timed out after {0:?}")]
    Timeout(Duration),
}

pub type StorageResult<T> = Result<T, StorageError>;
