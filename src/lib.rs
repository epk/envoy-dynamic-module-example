// Envoy 1.38.3's SDK macro compares its registered factory pointer for idempotence.
#![allow(unpredictable_function_pointer_comparisons)]

mod error;
mod exporter;
mod spanner;
mod telemetry;

use envoy_proxy_dynamic_modules_rust_sdk::{
    CatchUnwind, EnvoyBuffer, EnvoyHttpFilter, EnvoyHttpFilterConfig, EnvoyHttpFilterScheduler,
    HttpFilter, HttpFilterConfig, abi, declare_init_functions, envoy_log_error, envoy_log_info,
    envoy_log_warn, is_validation_mode,
};
use opentelemetry::Context;
use opentelemetry_sdk::trace::SdkTracer;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::{Builder, Handle, Runtime};
use uuid::Uuid;

use crate::error::StorageError;
use crate::spanner::SpannerClient;

declare_init_functions!(init, new_filter_config);

const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 10_000;
const RUNTIME_WORKER_THREADS: usize = 2;

fn init() -> bool {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return true;
    }

    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
        && rustls::crypto::CryptoProvider::get_default().is_none()
    {
        envoy_log_error!("failed to install the rustls AWS-LC crypto provider");
        return false;
    }

    true
}

fn new_filter_config<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    _envoy_filter_config: &mut EC,
    name: &str,
    config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    if name != "spanner_request_mapper" {
        envoy_log_error!("unsupported filter name: {name}");
        return None;
    }

    // SAFETY: Envoy invokes the config factory on its main thread, as required by this SDK API.
    let validation_only = unsafe { is_validation_mode() };
    match FilterConfig::new(config, validation_only) {
        Ok(config) => Some(Box::new(config)),
        Err(error) => {
            envoy_log_error!("failed to create filter config: {error}");
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationType {
    Request,
    Response,
}

impl OperationType {
    const fn event_bit(self) -> u64 {
        match self {
            Self::Request => 0,
            Self::Response => 1,
        }
    }

    const fn from_event_id(event_id: u64) -> Self {
        if event_id & 1 == 0 {
            Self::Request
        } else {
            Self::Response
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum OperationOutcome {
    Pending,
    Succeeded,
    Failed(String),
}

#[derive(Debug, Eq, PartialEq)]
struct PendingOperation {
    uuid: Uuid,
    operation_type: OperationType,
    outcome: OperationOutcome,
}

/// Results passed from Tokio tasks back to the Envoy worker thread.
#[derive(Clone)]
struct PendingRequests {
    map: Arc<Mutex<HashMap<u64, PendingOperation>>>,
    next_sequence: Arc<AtomicU64>,
}

impl PendingRequests {
    fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            next_sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, PendingOperation>> {
        self.map.lock().unwrap_or_else(|poisoned| {
            envoy_log_warn!("recovering a poisoned pending-operation lock");
            poisoned.into_inner()
        })
    }

    fn insert(&self, uuid: Uuid, operation_type: OperationType) -> u64 {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let event_id = (sequence << 1) | operation_type.event_bit();
        self.lock().insert(
            event_id,
            PendingOperation {
                uuid,
                operation_type,
                outcome: OperationOutcome::Pending,
            },
        );
        event_id
    }

    fn complete(&self, event_id: u64, result: Result<(), StorageError>) {
        if let Some(operation) = self.lock().get_mut(&event_id) {
            operation.outcome = match result {
                Ok(()) => OperationOutcome::Succeeded,
                Err(error) => OperationOutcome::Failed(error.to_string()),
            };
        }
    }

    fn remove(&self, event_id: u64) -> Option<PendingOperation> {
        self.lock().remove(&event_id)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModuleConfig {
    spanner: SpannerConfig,
    opentelemetry: exporter::Config,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpannerConfig {
    project_id: String,
    instance_id: String,
    database_id: String,
    #[serde(default = "default_operation_timeout_ms")]
    operation_timeout_ms: u64,
}

const fn default_operation_timeout_ms() -> u64 {
    DEFAULT_OPERATION_TIMEOUT_MS
}

#[derive(Debug, Error)]
enum ConfigError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("failed to create Tokio runtime: {0}")]
    Runtime(#[from] std::io::Error),

    #[error("failed to initialize Spanner: {0}")]
    Spanner(#[from] StorageError),

    #[error("failed to initialize OpenTelemetry: {0}")]
    OpenTelemetry(#[from] exporter::Error),

    #[error("{0} must not be empty")]
    EmptyField(&'static str),

    #[error("spanner.operation_timeout_ms must be greater than zero")]
    InvalidTimeout,
}

/// Immutable configuration shared by all Envoy worker threads.
struct FilterConfig {
    runtime: Option<Runtime>,
    spanner_client: Option<Arc<SpannerClient>>,
    telemetry: Option<exporter::Pipeline>,
    operation_timeout: Duration,
}

impl FilterConfig {
    fn new(config_json: &[u8], validation_only: bool) -> Result<Self, ConfigError> {
        let config: ModuleConfig = serde_json::from_slice(config_json)?;
        for (name, value) in [
            ("spanner.project_id", config.spanner.project_id.as_str()),
            ("spanner.instance_id", config.spanner.instance_id.as_str()),
            ("spanner.database_id", config.spanner.database_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::EmptyField(name));
            }
        }
        if config.spanner.operation_timeout_ms == 0 {
            return Err(ConfigError::InvalidTimeout);
        }
        config.opentelemetry.validate()?;
        let operation_timeout = Duration::from_millis(config.spanner.operation_timeout_ms);

        if validation_only {
            return Ok(Self {
                runtime: None,
                spanner_client: None,
                telemetry: None,
                operation_timeout,
            });
        }

        let runtime = Builder::new_multi_thread()
            .worker_threads(RUNTIME_WORKER_THREADS)
            .thread_name("envoy-spanner")
            .enable_all()
            .build()?;
        let spanner = runtime.block_on(SpannerClient::new(
            config.spanner.project_id,
            config.spanner.instance_id,
            config.spanner.database_id,
        ))?;
        let telemetry = {
            let _runtime_guard = runtime.enter();
            exporter::Pipeline::new(config.opentelemetry)?
        };

        Ok(Self {
            runtime: Some(runtime),
            spanner_client: Some(Arc::new(spanner)),
            telemetry: Some(telemetry),
            operation_timeout,
        })
    }
}

impl Drop for FilterConfig {
    fn drop(&mut self) {
        if let Some(telemetry) = self.telemetry.take()
            && let Err(error) = telemetry.shutdown(Duration::from_secs(2))
        {
            envoy_log_warn!("failed to shut down OpenTelemetry cleanly: {error}");
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for FilterConfig {
    fn new_http_filter(&self, _: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        let runtime = self
            .runtime
            .as_ref()
            .expect("validation-only config cannot create filters");
        let spanner = self
            .spanner_client
            .as_ref()
            .expect("validation-only config cannot create filters");
        let telemetry = self
            .telemetry
            .as_ref()
            .expect("validation-only config cannot create filters");

        Box::new(CatchUnwind::new(Filter {
            spanner: Arc::clone(spanner),
            runtime: runtime.handle().clone(),
            pending: PendingRequests::new(),
            request_id: None,
            operation_timeout: self.operation_timeout,
            tracer: telemetry.tracer(),
            exporter: telemetry.new_handle(),
            parent_context: None,
            deferred_operation: None,
        }))
    }
}

/// Per-request filter instance. Envoy invokes all of its callbacks on one worker thread.
struct Filter {
    spanner: Arc<SpannerClient>,
    runtime: Handle,
    pending: PendingRequests,
    request_id: Option<Uuid>,
    operation_timeout: Duration,
    tracer: SdkTracer,
    exporter: exporter::Handle,
    parent_context: Option<Context>,
    deferred_operation: Option<(u64, PendingOperation)>,
}

impl Filter {
    fn continue_after_missing_event<EHF: EnvoyHttpFilter>(envoy_filter: &mut EHF, event_id: u64) {
        envoy_log_error!("no pending operation for scheduled event {event_id}");
        match OperationType::from_event_id(event_id) {
            OperationType::Request => envoy_filter.continue_decoding(),
            OperationType::Response => envoy_filter.continue_encoding(),
        }
    }

    fn finish_operation<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        event_id: u64,
        operation: PendingOperation,
    ) {
        match (operation.operation_type, operation.outcome) {
            (OperationType::Request, OperationOutcome::Succeeded) => {
                self.request_id = Some(operation.uuid);
                if !envoy_filter
                    .set_request_header("x-request-id", operation.uuid.to_string().as_bytes())
                {
                    envoy_log_error!("failed to set x-request-id for {}", operation.uuid);
                }
                envoy_log_info!("stored request mapping for {}", operation.uuid);
                envoy_filter.continue_decoding();
            }
            (OperationType::Request, OperationOutcome::Failed(error)) => {
                envoy_log_error!(
                    "failed to store request mapping for {}: {error}",
                    operation.uuid
                );
                envoy_filter.continue_decoding();
            }
            (OperationType::Response, OperationOutcome::Succeeded) => {
                envoy_log_info!("stored response timestamp for {}", operation.uuid);
                envoy_filter.continue_encoding();
            }
            (OperationType::Response, OperationOutcome::Failed(error)) => {
                envoy_log_error!(
                    "failed to store response timestamp for {}: {error}",
                    operation.uuid
                );
                envoy_filter.continue_encoding();
            }
            (operation_type, OperationOutcome::Pending) => {
                envoy_log_error!("scheduled event {event_id} ran before its operation completed");
                match operation_type {
                    OperationType::Request => envoy_filter.continue_decoding(),
                    OperationType::Response => envoy_filter.continue_encoding(),
                }
            }
        }
    }
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
        let spanner = Arc::clone(&self.spanner);
        let pending = self.pending.clone();
        let timeout = self.operation_timeout;
        self.parent_context = match telemetry::active_parent_context(envoy_filter) {
            Ok(context) => Some(context),
            Err(error) => {
                envoy_log_warn!("cannot create Spanner child span: {error}");
                None
            }
        };
        let span = self.parent_context.as_ref().map(|parent| {
            telemetry::start_spanner_span(
                &self.tracer,
                parent,
                "spanner.insert_request_mapping",
                "INSERT",
                uuid,
            )
        });

        self.runtime.spawn(async move {
            let result = tokio::time::timeout(timeout, spanner.insert_request_mapping(uuid))
                .await
                .unwrap_or(Err(StorageError::Timeout(timeout)));
            telemetry::finish_span(span, &result);
            pending.complete(event_id, result);
            scheduler.commit(event_id);
        });

        abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopAllIterationAndWatermark
    }

    fn on_scheduled(&mut self, envoy_filter: &mut EHF, event_id: u64) {
        let Some(operation) = self.pending.remove(event_id) else {
            Self::continue_after_missing_event(envoy_filter, event_id);
            return;
        };

        if self.exporter.export_pending(envoy_filter) {
            self.deferred_operation = Some((event_id, operation));
        } else {
            self.finish_operation(envoy_filter, event_id, operation);
        }
    }

    fn on_response_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_response_headers_status {
        let Some(uuid) = self.request_id else {
            return abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue;
        };

        let event_id = self.pending.insert(uuid, OperationType::Response);
        let scheduler = envoy_filter.new_scheduler();
        let spanner = Arc::clone(&self.spanner);
        let pending = self.pending.clone();
        let timeout = self.operation_timeout;
        let span = self.parent_context.as_ref().map(|parent| {
            telemetry::start_spanner_span(
                &self.tracer,
                parent,
                "spanner.update_response_timestamp",
                "UPDATE",
                uuid,
            )
        });

        self.runtime.spawn(async move {
            let result = tokio::time::timeout(timeout, spanner.update_response_timestamp(uuid))
                .await
                .unwrap_or(Err(StorageError::Timeout(timeout)));
            telemetry::finish_span(span, &result);
            pending.complete(event_id, result);
            scheduler.commit(event_id);
        });

        abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::StopAllIterationAndWatermark
    }

    fn on_http_callout_done(
        &mut self,
        envoy_filter: &mut EHF,
        callout_id: u64,
        result: abi::envoy_dynamic_module_type_http_callout_result,
        response_headers: Option<&[(EnvoyBuffer, EnvoyBuffer)]>,
        _response_body: Option<&[EnvoyBuffer]>,
    ) {
        if self
            .exporter
            .on_http_callout_done(callout_id, result, response_headers)
            && let Some((event_id, operation)) = self.deferred_operation.take()
        {
            self.finish_operation(envoy_filter, event_id, operation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_requests_carry_results_back_to_the_correct_path() {
        let pending = PendingRequests::new();
        let request_uuid = Uuid::new_v4();
        let request_event = pending.insert(request_uuid, OperationType::Request);
        let response_uuid = Uuid::new_v4();
        let response_event = pending.insert(response_uuid, OperationType::Response);

        pending.complete(request_event, Ok(()));

        assert_eq!(
            pending.remove(request_event),
            Some(PendingOperation {
                uuid: request_uuid,
                operation_type: OperationType::Request,
                outcome: OperationOutcome::Succeeded,
            })
        );
        assert_eq!(
            OperationType::from_event_id(request_event),
            OperationType::Request
        );
        assert_eq!(
            OperationType::from_event_id(response_event),
            OperationType::Response
        );
    }

    #[test]
    fn config_validation_does_not_initialize_external_services() {
        let config = r#"{
            "spanner": {
                "project_id": "project",
                "instance_id": "instance",
                "database_id": "database"
            },
            "opentelemetry": {
                "service_name": "envoy-spanner-poc",
                "exporter": {
                    "type": "direct_grpc",
                    "grpc_endpoint": "http://127.0.0.1:4317"
                }
            }
        }"#;

        let config = FilterConfig::new(config.as_bytes(), true).unwrap();

        assert!(config.runtime.is_none());
        assert!(config.spanner_client.is_none());
        assert!(config.telemetry.is_none());
        assert_eq!(
            config.operation_timeout,
            Duration::from_millis(DEFAULT_OPERATION_TIMEOUT_MS)
        );
    }

    #[test]
    fn config_rejects_unknown_fields() {
        let config = r#"{
            "spanner": {
                "project_id": "project",
                "instance_id": "instance",
                "database_id": "database",
                "typo": true
            },
            "opentelemetry": {
                "service_name": "envoy-spanner-poc",
                "exporter": {
                    "type": "direct_grpc",
                    "grpc_endpoint": "http://127.0.0.1:4317"
                }
            }
        }"#;

        assert!(matches!(
            FilterConfig::new(config.as_bytes(), true),
            Err(ConfigError::Json(_))
        ));
    }

    #[test]
    fn config_rejects_empty_spanner_identifiers() {
        let config = br#"{
            "spanner": {
                "project_id": "",
                "instance_id": "instance",
                "database_id": "database"
            },
            "opentelemetry": {
                "service_name": "envoy-spanner-poc",
                "exporter": {
                    "type": "envoy_grpc_callout",
                    "cluster": "otel_collector_grpc"
                }
            }
        }"#;

        assert!(matches!(
            FilterConfig::new(config, true),
            Err(ConfigError::EmptyField("spanner.project_id"))
        ));
    }
}
