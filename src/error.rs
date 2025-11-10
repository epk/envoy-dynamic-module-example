use gcloud_gax::retry::TryAs;
use google_cloud_spanner::client::Error as SpannerError;
use google_cloud_spanner::session::SessionError;
use thiserror::Error;
use tonic::Status;

/// Error types for the Envoy Spanner extension
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Spanner client error: {0}")]
    Spanner(#[from] SpannerError),

    #[error("Session error: {0}")]
    Session(#[from] SessionError),

    #[error("gRPC status error: {0}")]
    Status(#[from] Status),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Transaction error: {0}")]
    Transaction(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Enable retry logic for Spanner errors
impl TryAs<Status> for StorageError {
    fn try_as(&self) -> Option<&Status> {
        match self {
            StorageError::Spanner(err) => err.try_as(),
            StorageError::Session(err) => err.try_as(),
            StorageError::Status(status) => Some(status),
            _ => None,
        }
    }
}
