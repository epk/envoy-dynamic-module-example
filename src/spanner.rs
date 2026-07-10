use crate::error::{StorageError, StorageResult};
use google_cloud_spanner::client::{Client, ClientConfig};
use google_cloud_spanner::mutation::{insert, update};
use google_cloud_spanner::value::CommitTimestamp;
use std::sync::Arc;
use uuid::Uuid;

const TABLE: &str = "request_mappings";

/// Spanner client wrapper for managing request ID mappings.
#[derive(Clone)]
pub struct SpannerClient {
    client: Arc<Client>,
}

impl SpannerClient {
    /// Create a new Spanner client using application-default credentials.
    pub async fn new(
        project_id: impl Into<String>,
        instance_id: impl Into<String>,
        database_id: impl Into<String>,
    ) -> StorageResult<Self> {
        let project_id = project_id.into();
        let instance_id = instance_id.into();
        let database_id = database_id.into();
        let database =
            format!("projects/{project_id}/instances/{instance_id}/databases/{database_id}");

        let config = ClientConfig::default()
            .with_auth()
            .await
            .map_err(|error| StorageError::Configuration(error.to_string()))?;
        let client = Client::new(database, config).await?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Insert the request mapping with a server-side commit timestamp.
    pub async fn insert_request_mapping(&self, request_id: Uuid) -> StorageResult<()> {
        let request_id = request_id.to_string();
        let commit_timestamp = CommitTimestamp::new();
        let mutation = insert(
            TABLE,
            &["request_id", "timestamp"],
            &[&request_id, &commit_timestamp],
        );

        self.client.apply(vec![mutation]).await?;
        Ok(())
    }

    /// Update the mapping with a server-side response commit timestamp.
    pub async fn update_response_timestamp(&self, request_id: Uuid) -> StorageResult<()> {
        let request_id = request_id.to_string();
        let commit_timestamp = CommitTimestamp::new();
        let mutation = update(
            TABLE,
            &["request_id", "response_timestamp"],
            &[&request_id, &commit_timestamp],
        );

        self.client.apply(vec![mutation]).await?;
        Ok(())
    }
}
