mod error;
mod spanner;

use envoy_proxy_dynamic_modules_rust_sdk::{
    abi, declare_init_functions, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter,
    HttpFilterConfig,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::{Handle, Runtime};
use uuid::Uuid;

use crate::spanner::SpannerClient;

declare_init_functions!(init, new_filter_config);

fn init() -> bool {
    true
}

fn new_filter_config<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    _envoy_filter_config: &mut EC,
    _name: &str,
    config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EC, EHF>>> {
    FilterConfig::new(std::str::from_utf8(config).unwrap_or("{}"))
        .map(|c| Box::new(c) as Box<dyn HttpFilterConfig<EC, EHF>>)
}

/// Type of Spanner operation
#[derive(Clone, Copy, PartialEq, Debug)]
enum OperationType {
    Request,
    Response,
}

/// Tracks in-flight Spanner operations
#[derive(Clone)]
struct PendingRequests {
    map: Arc<Mutex<HashMap<u64, (Uuid, OperationType)>>>,
    next_id: Arc<AtomicU64>,
}

impl PendingRequests {
    fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn insert(&self, uuid: Uuid, op_type: OperationType) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.map.lock().unwrap().insert(id, (uuid, op_type));
        id
    }

    fn remove(&self, id: u64) -> Option<(Uuid, OperationType)> {
        self.map.lock().unwrap().remove(&id)
    }
}

/// Spanner configuration
#[derive(Deserialize)]
#[allow(clippy::struct_field_names)]
struct SpannerConfig {
    project_id: String,
    instance_id: String,
    database_id: String,
}

/// Filter configuration (created once at module load)
#[derive(Deserialize)]
pub struct FilterConfig {
    spanner: SpannerConfig,
    #[serde(skip)]
    spanner_client: Option<Arc<SpannerClient>>,
    #[serde(skip)]
    runtime: Option<Handle>,
    #[serde(skip)]
    pending: Option<PendingRequests>,
}

impl FilterConfig {
    fn new(config_json: &str) -> Option<Self> {
        // Parse config
        let mut config: Self = serde_json::from_str(config_json).ok()?;

        // Setup TLS
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok()?;

        // Setup logging
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init()
            .ok();

        // Create Tokio runtime
        let runtime = Runtime::new().ok()?;
        let handle = runtime.handle().clone();

        // Initialize Spanner with config from YAML
        let spanner = runtime.block_on(async {
            SpannerClient::new(
                &config.spanner.project_id,
                &config.spanner.instance_id,
                &config.spanner.database_id,
            )
            .await
            .ok()
        })?;

        // Keep runtime alive
        std::mem::forget(runtime);

        config.spanner_client = Some(Arc::new(spanner));
        config.runtime = Some(handle);
        config.pending = Some(PendingRequests::new());

        Some(config)
    }
}

impl<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter> HttpFilterConfig<EC, EHF> for FilterConfig {
    fn new_http_filter(&mut self, _: &mut EC) -> Box<dyn HttpFilter<EHF>> {
        Box::new(Filter {
            spanner: self.spanner_client.clone().unwrap(),
            runtime: self.runtime.clone().unwrap(),
            pending: self.pending.clone().unwrap(),
            request_id: None,
        })
    }
}

/// Per-request filter instance
pub struct Filter {
    spanner: Arc<SpannerClient>,
    runtime: Handle,
    pending: PendingRequests,
    request_id: Option<Uuid>,
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for Filter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_headers_status {
        let uuid = Uuid::new_v4();
        let event_id = self.pending.insert(uuid, OperationType::Request);
        let scheduler = envoy_filter.new_scheduler();

        // Spawn async Spanner write
        let spanner = self.spanner.clone();
        let pending = self.pending.clone();
        self.runtime.spawn(async move {
            match spanner.insert_request_mapping(uuid).await {
                Ok(()) => {
                    tracing::info!("Inserted {uuid}, scheduling callback");
                    scheduler.commit(event_id);
                }
                Err(e) => {
                    tracing::error!("Spanner insert failed for {uuid}: {e}");
                    pending.remove(event_id);
                    scheduler.commit(event_id); // Continue anyway
                }
            }
        });

        abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    }

    fn on_scheduled(&mut self, envoy_filter: &mut EHF, event_id: u64) {
        if let Some((uuid, op_type)) = self.pending.remove(event_id) {
            match op_type {
                OperationType::Request => {
                    envoy_filter.set_request_header("x-request-id", uuid.to_string().as_bytes());
                    self.request_id = Some(uuid);
                    tracing::info!("Set header {uuid} after Spanner commit");
                    envoy_filter.continue_decoding();
                }
                OperationType::Response => {
                    tracing::info!("Response timestamp written for {uuid}, continuing response");
                    envoy_filter.continue_encoding();
                }
            }
        } else {
            tracing::warn!("No pending operation for event {event_id}");
            envoy_filter.continue_decoding();
        }
    }

    fn on_response_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_response_headers_status {
        if let Some(uuid) = self.request_id {
            let event_id = self.pending.insert(uuid, OperationType::Response);
            let scheduler = envoy_filter.new_scheduler();
            let spanner = self.spanner.clone();
            let pending = self.pending.clone();

            self.runtime.spawn(async move {
                match spanner.update_response_timestamp(uuid).await {
                    Ok(()) => {
                        tracing::info!("Updated response timestamp for {uuid}, scheduling callback");
                        scheduler.commit(event_id);
                    }
                    Err(e) => {
                        tracing::error!("Failed to update response timestamp for {uuid}: {e}");
                        pending.remove(event_id);
                        scheduler.commit(event_id); // Continue anyway
                    }
                }
            });

            abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::StopIteration
        } else {
            abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_requests() {
        let pending = PendingRequests::new();
        let uuid = Uuid::new_v4();
        let id = pending.insert(uuid, OperationType::Request);
        assert_eq!(pending.remove(id), Some((uuid, OperationType::Request)));
        assert_eq!(pending.remove(id), None);
    }
}
