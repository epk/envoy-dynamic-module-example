use envoy_proxy_dynamic_modules_rust_sdk::EnvoyHttpFilter;
use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId,
    TraceState, Tracer as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::{SdkTracer, Span as SdkSpan};
use uuid::Uuid;

use crate::error::StorageResult;

/// Copy the active Envoy span identity into an owned OpenTelemetry context.
/// The owned context can safely outlive the HTTP callback and cross into Tokio.
pub fn active_parent_context<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> Result<Context, String> {
    let active_span = envoy_filter
        .get_active_span()
        .ok_or_else(|| "Envoy has no active span".to_string())?;
    let trace_id = active_span
        .get_trace_id()
        .ok_or_else(|| "active Envoy span has no trace ID".to_string())?;
    let span_id = active_span
        .get_span_id()
        .ok_or_else(|| "active Envoy span has no span ID".to_string())?;

    parent_context(&trace_id, &span_id)
}

fn parent_context(trace_id: &str, span_id: &str) -> Result<Context, String> {
    let trace_id =
        TraceId::from_hex(trace_id).map_err(|error| format!("invalid Envoy trace ID: {error}"))?;
    let span_id =
        SpanId::from_hex(span_id).map_err(|error| format!("invalid Envoy span ID: {error}"))?;
    let span_context = SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );

    Ok(Context::new().with_remote_span_context(span_context))
}

pub fn start_spanner_span(
    tracer: &SdkTracer,
    parent: &Context,
    name: &'static str,
    operation: &'static str,
    request_id: Uuid,
) -> SdkSpan {
    tracer
        .span_builder(name)
        .with_kind(SpanKind::Client)
        .with_attributes(vec![
            KeyValue::new("db.system.name", "gcp.spanner"),
            KeyValue::new("db.operation.name", operation),
            KeyValue::new("request.id", request_id.to_string()),
        ])
        .start_with_context(tracer, parent)
}

pub fn finish_span(mut span: Option<SdkSpan>, result: &StorageResult<()>) {
    let Some(span) = span.as_mut() else {
        return;
    };

    if let Err(error) = result {
        span.record_error(error);
        span.set_status(Status::error(error.to_string()));
    }
    span.end();
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    #[test]
    fn exported_span_is_a_child_of_the_copied_envoy_span() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let parent =
            parent_context("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7").unwrap();
        let mut span = start_spanner_span(
            &tracer,
            &parent,
            "spanner.insert_request_mapping",
            "INSERT",
            Uuid::nil(),
        );

        span.end();
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(spans[0].parent_span_id.to_string(), "00f067aa0ba902b7");
    }
}
