//! OTLP/gRPC export through Envoy HTTP streams.
//!
//! Envoy exposes request-level HTTP streams rather than a tonic `Channel`. This is the thin
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
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use tonic::Code;
use tonic::codec::{BufferSettings, EncodeBody};
use tonic_prost::ProstCodec;

const STREAM_TIMEOUT_MS: u64 = 1_000;
const MAX_PENDING_EXPORTS: usize = 2_048;
const OTLP_GRPC_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

pub(super) fn build_provider(
    cluster: String,
    resource: Resource,
) -> (SdkTracerProvider, HandleFactory) {
    let pending = PendingFrames::default();
    let exporter = EnvoyGrpcExporter::new(pending.clone());
    // Queue the frame before the request's scheduler wakes Envoy. A batch processor would export
    // later on its own thread, where no request-scoped ABI handle is available to send it.
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
            state: Some(StreamHandle {
                cluster: Arc::clone(&self.cluster),
                pending: self.pending.clone(),
                in_flight: HashMap::new(),
            }),
        }
    }
}

/// Per-stream export hook. In direct mode its methods intentionally do nothing.
pub struct Handle {
    state: Option<StreamHandle>,
}

impl Handle {
    pub(super) const fn direct() -> Self {
        Self { state: None }
    }

    /// Returns true when the downstream stream must stay alive for an Envoy stream callback.
    pub fn export_pending<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) -> bool {
        self.state
            .as_mut()
            .is_some_and(|state| state.export_pending(envoy_filter))
    }

    pub fn on_http_stream_headers(
        &mut self,
        stream_id: u64,
        headers: &[(EnvoyBuffer, EnvoyBuffer)],
        end_stream: bool,
    ) {
        if let Some(state) = self.state.as_mut() {
            state.on_http_stream_headers(stream_id, headers, end_stream);
        }
    }

    pub fn on_http_stream_trailers(
        &mut self,
        stream_id: u64,
        trailers: &[(EnvoyBuffer, EnvoyBuffer)],
    ) {
        if let Some(state) = self.state.as_mut() {
            state.on_http_stream_trailers(stream_id, trailers);
        }
    }

    /// Returns true when this was the last stream owned by this handle.
    pub fn on_http_stream_complete(&mut self, stream_id: u64) -> bool {
        self.state
            .as_mut()
            .is_some_and(|state| state.on_http_stream_complete(stream_id))
    }

    /// Returns true when this was the last stream owned by this handle.
    pub fn on_http_stream_reset(
        &mut self,
        stream_id: u64,
        reason: abi::envoy_dynamic_module_type_http_stream_reset_reason,
    ) -> bool {
        self.state
            .as_mut()
            .is_some_and(|state| state.on_http_stream_reset(stream_id, reason))
    }
}

struct StreamHandle {
    cluster: Arc<str>,
    pending: PendingFrames,
    in_flight: HashMap<u64, GrpcResponse>,
}

impl StreamHandle {
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
            let (result, stream_id) = envoy_filter.start_http_stream(
                &self.cluster,
                &headers,
                Some(&frame),
                true,
                STREAM_TIMEOUT_MS,
            );
            if result == abi::envoy_dynamic_module_type_http_callout_init_result::Success {
                self.in_flight.insert(stream_id, GrpcResponse::default());
            } else {
                envoy_log_warn!(
                    "failed to start OTLP/gRPC Envoy stream to {}: {result:?}",
                    self.cluster
                );
                frames.push_front(frame);
                self.pending.prepend(frames);
                break;
            }
        }
        !self.in_flight.is_empty()
    }

    fn on_http_stream_headers(
        &mut self,
        stream_id: u64,
        headers: &[(EnvoyBuffer, EnvoyBuffer)],
        end_stream: bool,
    ) {
        if let Some(response) = self.in_flight.get_mut(&stream_id) {
            response.record_headers(headers, end_stream);
        }
    }

    fn on_http_stream_trailers(&mut self, stream_id: u64, trailers: &[(EnvoyBuffer, EnvoyBuffer)]) {
        if let Some(response) = self.in_flight.get_mut(&stream_id) {
            response.record_trailers(trailers);
        }
    }

    fn on_http_stream_complete(&mut self, stream_id: u64) -> bool {
        let Some(response) = self.in_flight.remove(&stream_id) else {
            return false;
        };
        match response.validate() {
            Ok(()) => envoy_log_info!("exported module span through Envoy stream {stream_id}"),
            Err(error) => envoy_log_warn!("OTLP/gRPC Envoy stream {stream_id} failed: {error}"),
        }
        self.in_flight.is_empty()
    }

    fn on_http_stream_reset(
        &mut self,
        stream_id: u64,
        reason: abi::envoy_dynamic_module_type_http_stream_reset_reason,
    ) -> bool {
        if self.in_flight.remove(&stream_id).is_none() {
            return false;
        }
        envoy_log_warn!("OTLP/gRPC Envoy stream {stream_id} reset: {reason:?}");
        self.in_flight.is_empty()
    }
}

#[derive(Debug, Default)]
struct GrpcResponse {
    http_status: Option<Vec<u8>>,
    grpc_status: Option<Code>,
}

impl GrpcResponse {
    fn record_headers(&mut self, headers: &[(EnvoyBuffer, EnvoyBuffer)], end_stream: bool) {
        self.http_status = owned_header_value(headers, b":status");
        if end_stream {
            self.record_status(headers);
        }
    }

    fn record_trailers(&mut self, trailers: &[(EnvoyBuffer, EnvoyBuffer)]) {
        self.record_status(trailers);
    }

    fn record_status(&mut self, metadata: &[(EnvoyBuffer, EnvoyBuffer)]) {
        if let Some(status) = header_value(metadata, b"grpc-status") {
            self.grpc_status = Some(Code::from_bytes(status));
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.http_status.as_deref() != Some(b"200") {
            return Err(format!(
                "HTTP status {}",
                display_header(self.http_status.as_deref())
            ));
        }
        match self.grpc_status {
            Some(Code::Ok) => Ok(()),
            Some(status) => Err(format!("gRPC status {status:?}")),
            None => Err("missing grpc-status".to_string()),
        }
    }
}

fn display_header(value: Option<&[u8]>) -> String {
    value.map_or("<missing>".to_string(), |value| {
        String::from_utf8_lossy(value).into_owned()
    })
}

fn header_value<'a>(headers: &'a [(EnvoyBuffer, EnvoyBuffer)], name: &[u8]) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|(key, _)| key.as_slice().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_slice())
}

fn owned_header_value(headers: &[(EnvoyBuffer, EnvoyBuffer)], name: &[u8]) -> Option<Vec<u8>> {
    header_value(headers, name).map(<[u8]>::to_vec)
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
        if frames.len() >= MAX_PENDING_EXPORTS {
            return Err(OTelSdkError::InternalFailure(format!(
                "Envoy OTLP/gRPC export queue reached {MAX_PENDING_EXPORTS} entries"
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
    use envoy_proxy_dynamic_modules_rust_sdk::MockEnvoyHttpFilter;
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

    #[test]
    fn grpc_response_requires_success_status_in_trailers() {
        let headers = [
            (EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"200")),
            (
                EnvoyBuffer::new(b"content-type"),
                EnvoyBuffer::new(b"application/grpc"),
            ),
        ];
        let trailers = [(EnvoyBuffer::new(b"grpc-status"), EnvoyBuffer::new(b"0"))];
        let mut response = GrpcResponse::default();

        response.record_headers(&headers, false);
        assert_eq!(response.validate().unwrap_err(), "missing grpc-status");

        response.record_trailers(&trailers);
        assert_eq!(response.validate(), Ok(()));
    }

    #[test]
    fn grpc_response_rejects_nonzero_and_missing_http_statuses() {
        let trailers = [(EnvoyBuffer::new(b"grpc-status"), EnvoyBuffer::new(b"14"))];
        let mut response = GrpcResponse::default();

        response.record_trailers(&trailers);
        assert_eq!(response.validate().unwrap_err(), "HTTP status <missing>");

        let headers = [(EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"200"))];
        response.record_headers(&headers, false);
        assert_eq!(response.validate().unwrap_err(), "gRPC status Unavailable");
    }

    #[test]
    fn grpc_trailers_only_response_reads_status_from_headers() {
        let headers = [
            (EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"200")),
            (EnvoyBuffer::new(b"grpc-status"), EnvoyBuffer::new(b"0")),
        ];
        let mut response = GrpcResponse::default();

        response.record_headers(&headers, true);

        assert_eq!(response.validate(), Ok(()));
    }

    #[test]
    fn pending_frame_uses_envoy_stream_and_completes_after_trailers() {
        let pending = PendingFrames::default();
        pending.push(vec![0, 0, 0, 0, 0]).unwrap();
        let mut handle = StreamHandle {
            cluster: Arc::from("collector"),
            pending,
            in_flight: HashMap::new(),
        };
        let mut envoy = MockEnvoyHttpFilter::default();
        envoy.expect_start_http_stream().times(1).return_const((
            abi::envoy_dynamic_module_type_http_callout_init_result::Success,
            42,
        ));

        assert!(handle.export_pending(&mut envoy));
        assert!(handle.in_flight.contains_key(&42));

        let headers = [(EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"200"))];
        let trailers = [(EnvoyBuffer::new(b"grpc-status"), EnvoyBuffer::new(b"0"))];
        handle.on_http_stream_headers(42, &headers, false);
        handle.on_http_stream_trailers(42, &trailers);

        assert!(handle.on_http_stream_complete(42));
        assert!(handle.in_flight.is_empty());
    }
}
