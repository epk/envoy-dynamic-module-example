# Envoy Spanner Extension

Proof-of-concept HTTP filter built against Envoy 1.38.3 and dynamic modules ABI v0.1.0. It:

- Generates a UUIDv4 for each request
- Writes request and response timestamps to Google Cloud Spanner
- Adds `x-request-id` after the request mapping commits
- Exports each Spanner operation as a child of Envoy's active OpenTelemetry span

Envoy dynamic modules have strict compatibility requirements. The Rust SDK revision in
`Cargo.toml` exactly matches Envoy 1.38.3; rebuild the module whenever the target Envoy minor
version changes.

## Prerequisites

- Rust 1.97.0 (pinned by `rust-toolchain.toml`)
- Envoy 1.38.3
- Google application-default credentials with access to the configured Spanner database
- Docker, if you want to run the included local OpenTelemetry Collector example

On macOS, the current Homebrew formula installs the matching Envoy release:

```bash
brew install envoy
envoy --version
```

Create the Spanner table:

```sql
CREATE TABLE request_mappings (
  request_id STRING(36) NOT NULL,
  timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  response_timestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (request_id);
```

Update the Spanner identifiers in `envoy-config.yaml`.

`operation_timeout_ms` bounds each request or response write so a stalled Spanner operation cannot
leave the Envoy stream paused indefinitely. It defaults to 10 seconds.

Build the module:

```bash
cargo build --release --locked
```

The example also enables Envoy's native OpenTelemetry tracer at 100% sampling. Envoy exports its
server and upstream client spans to `otel_collector_grpc` over OTLP/gRPC, and propagates W3C
`traceparent` and `tracestate` headers to the upstream.

## Why this tracing POC exists

The HTTP dynamic-module ABI exposes the active Envoy trace ID and span ID. Those two copied strings
are enough for the module to create a correctly parented, owned OpenTelemetry span around work that
runs on Tokio. This matters because Envoy's native child-span handle is tied to the current HTTP
callback and is not `Send`, so safe Rust cannot carry that handle through the asynchronous Spanner
operation.

The module supports two OTLP/gRPC exporters on port 4317. The only difference is who owns the
network connection:

- `direct_grpc` (the default in `envoy-config.yaml`) lets the Rust OpenTelemetry SDK dial the
  collector directly.
- `envoy_grpc_callout` sends the same OTLP/gRPC request through Envoy's
  `otel_collector_grpc` cluster, so Envoy owns the connection.

Span creation is identical in both modes. Export selection lives in `src/exporter.rs`, the two
implementations live in `src/exporter/direct.rs` and `src/exporter/envoy.rs`, and the callout
adapter delegates OTLP conversion to OpenTelemetry, gRPC framing to tonic, and transport to the
Envoy ABI.

To try the Envoy callout path, replace the `exporter` object in the dynamic-module configuration:

```json
"exporter": {
  "type": "envoy_grpc_callout",
  "cluster": "otel_collector_grpc"
}
```

Envoy's native tracer also uses `otel_collector_grpc`, regardless of which exporter the module
uses.

Envoy discovers dynamic modules as `lib{name}.so`. Cargo uses the `.dylib` suffix on macOS, so
create the expected symlink once after building:

```bash
ln -sf libenvoy_spanner_extension.dylib \
  target/release/libenvoy_spanner_extension.so
```

Validate the module and configuration without contacting Spanner:

```bash
ENVOY_DYNAMIC_MODULES_SEARCH_PATH=target/release \
envoy --mode validate -c envoy-config.yaml
```

Start a local collector in another terminal. The included configuration accepts OTLP/gRPC and
prints detailed spans to the collector log:

```bash
docker run --rm --name envoy-spanner-otel \
  -p 4317:4317 \
  -v "$PWD/otel-collector-config.yaml:/etc/otelcol/config.yaml:ro" \
  otel/opentelemetry-collector:0.156.0 \
  --config=/etc/otelcol/config.yaml
```

Run Envoy:

```bash
ENVOY_DYNAMIC_MODULES_SEARCH_PATH=target/release \
envoy --log-level info -c envoy-config.yaml
```

Send a request with a fixed, sampled W3C parent context so the trace is easy to find in the
collector output:

```bash
curl -v \
  -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' \
  http://localhost:10000/get
```

The collector should report Envoy's server and upstream client spans plus the module's Spanner
spans, all with trace ID `4bf92f3577b34da6a3ce929d0e0e4736`. Each Spanner span's parent ID
should equal the Envoy server span ID. The Postman Echo response also shows the `traceparent` Envoy
propagated upstream. Finally, check the Envoy `dynamic_modules` logs and verify the row in Spanner.

## Test

```bash
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets --all-features -- -D warnings -W clippy::pedantic
```
