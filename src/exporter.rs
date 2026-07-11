//! Export transport selection for the tracing POC.
//!
//! Span creation is deliberately independent of this module. Both transports consume the same
//! OpenTelemetry SDK spans; only ownership of the OTLP/gRPC connection changes.

mod direct;
mod envoy;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;

pub use envoy::Handle;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    service_name: String,
    exporter: Mode,
}

impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        if self.service_name.trim().is_empty() {
            return Err(Error::EmptyServiceName);
        }
        self.exporter.validate()
    }
}

/// Both variants speak OTLP/gRPC. The only difference is whether tonic or Envoy owns the
/// collector connection.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Mode {
    DirectGrpc { grpc_endpoint: String },
    EnvoyGrpcCallout { cluster: String },
}

impl Mode {
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::DirectGrpc { grpc_endpoint } => {
                if grpc_endpoint.trim().is_empty() {
                    return Err(Error::EmptyField("grpc_endpoint"));
                }
                if !grpc_endpoint.starts_with("http://") && !grpc_endpoint.starts_with("https://") {
                    return Err(Error::InvalidGrpcEndpoint);
                }
            }
            Self::EnvoyGrpcCallout { cluster } => {
                if cluster.trim().is_empty() {
                    return Err(Error::EmptyField("cluster"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to build the direct OTLP/gRPC exporter: {0}")]
    Direct(#[from] opentelemetry_otlp::ExporterBuildError),

    #[error("opentelemetry.exporter.{0} must not be empty")]
    EmptyField(&'static str),

    #[error("opentelemetry.exporter.grpc_endpoint must use http:// or https://")]
    InvalidGrpcEndpoint,

    #[error("opentelemetry.service_name must not be empty")]
    EmptyServiceName,
}

/// Owns the SDK provider and the transport-specific factory for per-stream handles.
pub struct Pipeline {
    provider: SdkTracerProvider,
    handle_factory: HandleFactory,
}

impl Pipeline {
    /// Direct tonic setup must run while entered into a Tokio runtime.
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let resource = Resource::builder()
            .with_service_name(config.service_name)
            .build();

        let (provider, handle_factory) = match config.exporter {
            Mode::DirectGrpc { grpc_endpoint } => (
                direct::build_provider(grpc_endpoint, resource)?,
                HandleFactory::Direct,
            ),
            Mode::EnvoyGrpcCallout { cluster } => {
                let (provider, factory) = envoy::build_provider(cluster, resource);
                (provider, HandleFactory::Envoy(factory))
            }
        };

        Ok(Self {
            provider,
            handle_factory,
        })
    }

    pub fn tracer(&self) -> SdkTracer {
        self.provider.tracer("envoy-spanner-extension")
    }

    pub fn new_handle(&self) -> Handle {
        self.handle_factory.new_handle()
    }

    pub fn shutdown(&self, timeout: Duration) -> OTelSdkResult {
        self.provider.shutdown_with_timeout(timeout)
    }
}

enum HandleFactory {
    Direct,
    Envoy(envoy::HandleFactory),
}

impl HandleFactory {
    fn new_handle(&self) -> Handle {
        match self {
            Self::Direct => Handle::direct(),
            Self::Envoy(factory) => factory.new_handle(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exporter_config_validates_transport_specific_fields() {
        assert!(
            Mode::DirectGrpc {
                grpc_endpoint: "collector:4317".to_string()
            }
            .validate()
            .is_err()
        );
        assert!(
            Mode::EnvoyGrpcCallout {
                cluster: " ".to_string()
            }
            .validate()
            .is_err()
        );
    }
}
