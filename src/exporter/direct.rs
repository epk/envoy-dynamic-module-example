//! Direct OTLP/gRPC export using tonic-owned connections.

use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use std::time::Duration;

const EXPORT_INTERVAL: Duration = Duration::from_millis(100);
const EXPORT_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) fn build_provider(
    grpc_endpoint: String,
    resource: Resource,
) -> Result<SdkTracerProvider, opentelemetry_otlp::ExporterBuildError> {
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(grpc_endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()?;
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_scheduled_delay(EXPORT_INTERVAL)
                .build(),
        )
        .build();

    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_span_processor(processor)
        .build())
}
