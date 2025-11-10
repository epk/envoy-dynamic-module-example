use crate::error::{StorageError, StorageResult};
use google_cloud_spanner::client::{Client, ClientConfig};
use google_cloud_spanner::statement::Statement;
use std::sync::Arc;
use uuid::Uuid;

const INSERT_SQL: &str = r"
    INSERT INTO request_mappings
    (request_id, timestamp)
    VALUES (@request_id, PENDING_COMMIT_TIMESTAMP())
";

const UPDATE_RESPONSE_SQL: &str = r"
    UPDATE request_mappings
    SET response_timestamp = PENDING_COMMIT_TIMESTAMP()
    WHERE request_id = @request_id
";

/// Spanner client wrapper for managing request ID mappings
#[derive(Clone)]
pub struct SpannerClient {
    client: Arc<Client>,
}

impl SpannerClient {
    /// Create a new Spanner client with explicit configuration
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

        // Configure authentication using default credentials
        let config = ClientConfig::default()
            .with_auth()
            .await
            .map_err(|e| StorageError::Configuration(e.to_string()))?;

        let client = Client::new(database, config).await?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Insert a request ID mapping into Spanner
    ///
    /// This performs a synchronous write and waits for transaction commit.
    /// The timestamp is automatically set by Spanner using `PENDING_COMMIT_TIMESTAMP()`.
    pub async fn insert_request_mapping(&self, request_id: Uuid) -> StorageResult<()> {
        let request_id_str = request_id.to_string();

        let (_, result) = self
            .client
            .read_write_transaction(|tx| {
                let request_id_str = request_id_str.clone();
                Box::pin(async move {
                    let mut stmt = Statement::new(INSERT_SQL);
                    stmt.add_param("request_id", &request_id_str);

                    tx.update(stmt).await.map_err(|e| {
                        StorageError::Transaction(format!("Failed to insert request mapping: {e}"))
                    })?;

                    Ok::<_, StorageError>(())
                })
            })
            .await?;

        Ok(result)
    }

    /// Update the response timestamp for a request ID mapping
    ///
    /// This performs a synchronous write and waits for transaction commit.
    /// The response timestamp is automatically set by Spanner using `PENDING_COMMIT_TIMESTAMP()`.
    pub async fn update_response_timestamp(&self, request_id: Uuid) -> StorageResult<()> {
        let request_id_str = request_id.to_string();

        let (_, result) = self
            .client
            .read_write_transaction(|tx| {
                let request_id_str = request_id_str.clone();
                Box::pin(async move {
                    let mut stmt = Statement::new(UPDATE_RESPONSE_SQL);
                    stmt.add_param("request_id", &request_id_str);

                    tx.update(stmt).await.map_err(|e| {
                        StorageError::Transaction(format!("Failed to update response timestamp: {e}"))
                    })?;

                    Ok::<_, StorageError>(())
                })
            })
            .await?;

        Ok(result)
    }
}
