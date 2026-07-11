//! OTLP/gRPC export through Envoy HTTP callouts.
//!
//! Envoy exposes request-level HTTP callouts rather than a tonic `Channel`. This is the thin
//! adapter between them: OpenTelemetry builds `SpanData`, its OTLP transform builds the request,
//! tonic frames it, and this module only queues that body until an Envoy worker can send it.

use envoy_proxy_dynamic_modules_rust_sdk::{
    EnvoyBuffer, EnvoyHttpFilter, abi, envoy_log_info, envoy_log_warn,
};
use http_body_util::BodyExt as _;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema;
use opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use tonic::codec::{BufferSettings, EncodeBody};
use tonic_prost::ProstCodec;

const CALLOUT_TIMEOUT_MS: u64 = 1_000;
const MAX_PENDING_CALLOUTS: usize = 2_048;
const OTLP_GRPC_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

pub(super) fn build_provider(
    cluster: String,
    resource: Resource,
) -> (SdkTracerProvider, HandleFactory) {
    let pending = PendingFrames::default();
    let exporter = EnvoyGrpcExporter::new(pending.clone());
    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_simple_exporter(exporter)
        .build();
    let factory = HandleFactory {
        cluster: Arc::from(cluster),
        pending,
    };

    (provider, factory)
}

pub(super) struct HandleFactory {
    cluster: Arc<str>,
    pending: PendingFrames,
}

impl HandleFactory {
    pub(super) fn new_handle(&self) -> Handle {
        Handle {
            state: Some(CalloutHandle {
                cluster: Arc::clone(&self.cluster),
                pending: self.pending.clone(),
                in_flight: HashSet::new(),
            }),
        }
    }
}

/// Per-stream export hook. In direct mode its methods intentionally do nothing.
pub struct Handle {
    state: Option<CalloutHandle>,
}

impl Handle {
    pub(super) const fn direct() -> Self {
        Self { state: None }
    }

    /// Returns true when the stream must stay alive for an Envoy callout callback.
    pub fn export_pending<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) -> bool {
        self.state
            .as_mut()
            .is_some_and(|state| state.export_pending(envoy_filter))
    }

    pub fn on_http_callout_done(
        &mut self,
        callout_id: u64,
        result: abi::envoy_dynamic_module_type_http_callout_result,
        response_headers: Option<&[(EnvoyBuffer, EnvoyBuffer)]>,
    ) -> bool {
        self.state
            .as_mut()
            .is_some_and(|state| state.on_http_callout_done(callout_id, result, response_headers))
    }
}

struct CalloutHandle {
    cluster: Arc<str>,
    pending: PendingFrames,
    in_flight: HashSet<u64>,
}

impl CalloutHandle {
    fn export_pending<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) -> bool {
        let mut frames = self.pending.drain();
        while let Some(frame) = frames.pop_front() {
            let headers: [(&str, &[u8]); 5] = [
                (":method", b"POST"),
                (":path", OTLP_GRPC_PATH.as_bytes()),
                ("host", self.cluster.as_bytes()),
                ("content-type", b"application/grpc"),
                ("te", b"trailers"),
            ];
            let (result, callout_id) = envoy_filter.send_http_callout(
                &self.cluster,
                &headers,
                Some(&frame),
                CALLOUT_TIMEOUT_MS,
            );
            if result == abi::envoy_dynamic_module_type_http_callout_init_result::Success {
                self.in_flight.insert(callout_id);
            } else {
                envoy_log_warn!(
                    "failed to start OTLP/gRPC Envoy callout to {}: {result:?}",
                    self.cluster
                );
                frames.push_front(frame);
                self.pending.prepend(frames);
                break;
            }
        }
        !self.in_flight.is_empty()
    }

    fn on_http_callout_done(
        &mut self,
        callout_id: u64,
        result: abi::envoy_dynamic_module_type_http_callout_result,
        response_headers: Option<&[(EnvoyBuffer, EnvoyBuffer)]>,
    ) -> bool {
        if !self.in_flight.remove(&callout_id) {
            return self.in_flight.is_empty();
        }
        if result == abi::envoy_dynamic_module_type_http_callout_result::Success {
            let http_status =
                response_headers.and_then(|headers| header_value(headers, b":status"));
            let grpc_status =
                response_headers.and_then(|headers| header_value(headers, b"grpc-status"));
            if http_status != Some(b"200".as_slice()) {
                envoy_log_warn!(
                    "OTLP/gRPC Envoy callout {callout_id} returned HTTP {}",
                    display_header(http_status)
                );
            } else if grpc_status.is_some_and(|status| status != b"0") {
                envoy_log_warn!(
                    "OTLP/gRPC Envoy callout {callout_id} returned grpc-status {}",
                    display_header(grpc_status)
                );
            } else {
                envoy_log_info!("exported module span through Envoy callout {callout_id}");
            }
        } else {
            envoy_log_warn!("OTLP/gRPC Envoy callout {callout_id} failed: {result:?}");
        }
        self.in_flight.is_empty()
    }
}

fn display_header(value: Option<&[u8]>) -> String {
    value.map_or("<missing>".to_string(), |value| {
        String::from_utf8_lossy(value).into_owned()
    })
}

fn header_value<'a>(
    headers: &'a [(EnvoyBuffer<'a>, EnvoyBuffer<'a>)],
    name: &[u8],
) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|(key, _)| key.as_slice().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_slice())
}

#[derive(Clone, Debug, Default)]
struct PendingFrames {
    frames: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl PendingFrames {
    fn lock(&self) -> MutexGuard<'_, VecDeque<Vec<u8>>> {
        self.frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn push(&self, frame: Vec<u8>) -> OTelSdkResult {
        let mut frames = self.lock();
        if frames.len() >= MAX_PENDING_CALLOUTS {
            return Err(OTelSdkError::InternalFailure(format!(
                "Envoy OTLP/gRPC callout queue reached {MAX_PENDING_CALLOUTS} entries"
            )));
        }
        frames.push_back(frame);
        Ok(())
    }

    fn drain(&self) -> VecDeque<Vec<u8>> {
        std::mem::take(&mut *self.lock())
    }

    fn prepend(&self, frames: VecDeque<Vec<u8>>) {
        let mut pending = self.lock();
        for frame in frames.into_iter().rev() {
            pending.push_front(frame);
        }
    }
}

#[derive(Debug)]
struct EnvoyGrpcExporter {
    pending: PendingFrames,
    resource: ResourceAttributesWithSchema,
}

impl EnvoyGrpcExporter {
    fn new(pending: PendingFrames) -> Self {
        Self {
            pending,
            resource: ResourceAttributesWithSchema::default(),
        }
    }
}

impl SpanExporter for EnvoyGrpcExporter {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let request = ExportTraceServiceRequest {
            resource_spans: group_spans_by_resource_and_scope(batch, &self.resource),
        };
        let pending = self.pending.clone();
        async move {
            let frame = encode_grpc_request(request).await?;
            pending.push(frame)
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
}

async fn encode_grpc_request(request: ExportTraceServiceRequest) -> Result<Vec<u8>, OTelSdkError> {
    let encoder = ProstCodec::<ExportTraceServiceRequest, ExportTraceServiceResponse>::raw_encoder(
        BufferSettings::default(),
    );
    let messages = tokio_stream::once(Ok::<_, tonic::Status>(request));
    let body = EncodeBody::new_client(encoder, messages, None, None);
    let collected_body = body.collect().await.map_err(|error| {
        OTelSdkError::InternalFailure(format!("failed to encode OTLP/gRPC request: {error}"))
    })?;
    Ok(collected_body.to_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{Span as _, Tracer as _, TracerProvider as _};
    use prost::Message as _;

    #[test]
    fn sdk_span_becomes_a_tonic_framed_otlp_request() {
        let pending = PendingFrames::default();
        let exporter = EnvoyGrpcExporter::new(pending.clone());
        let provider = SdkTracerProvider::builder()
            .with_resource(Resource::builder_empty().with_service_name("test").build())
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer("test");
        let mut span = tracer.start("spanner.test");

        span.end();

        let frame = pending.drain().pop_front().unwrap();

        assert_eq!(frame[0], 0);
        let payload_length = u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize;
        assert_eq!(payload_length, frame.len() - 5);
        let request = ExportTraceServiceRequest::decode(&frame[5..]).unwrap();
        assert_eq!(
            request.resource_spans[0].scope_spans[0].spans[0].name,
            "spanner.test"
        );
    }
}
