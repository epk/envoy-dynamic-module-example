# Envoy Spanner Extension

Proof-of-concept HTTP filter built against Envoy 1.38.3 and dynamic modules ABI v0.1.0. It:

- Generates a UUIDv4 for each request
- Writes request and response timestamps to Google Cloud Spanner
- Adds `x-request-id` after the request mapping commits

Envoy dynamic modules have strict compatibility requirements. The Rust SDK revision in
`Cargo.toml` exactly matches Envoy 1.38.3; rebuild the module whenever the target Envoy minor
version changes.

## Prerequisites

- Rust 1.97.0 (pinned by `rust-toolchain.toml`)
- Envoy 1.38.3
- Google application-default credentials with access to the configured Spanner database

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

Update the Spanner identifiers in `envoy-config.yaml`, then build the module:

`operation_timeout_ms` bounds each request or response write so a stalled Spanner operation cannot
leave the Envoy stream paused indefinitely. It defaults to 10 seconds.

```bash
cargo build --release --locked
```

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

Run Envoy:

```bash
ENVOY_DYNAMIC_MODULES_SEARCH_PATH=target/release \
envoy --log-level info -c envoy-config.yaml
```

Send a request:

```bash
curl -v http://localhost:10000/get
```

Check the Envoy `dynamic_modules` logs for the UUID and Spanner commits, then verify the row in
Spanner.

## Test

```bash
cargo test --locked
```
