# Envoy Spanner Extension

Proof of concept Envoy Dynamic Module (v1.35+) that:
- Generates UUIDv4 for each request
- Writes request/response timestamps to Google Cloud Spanner
- Adds `x-request-id` header after async Spanner commit

## Prerequisites

```bash
brew install envoy
```

Spanner table:
```sql
CREATE TABLE request_mappings (
  request_id STRING(36) NOT NULL,
  timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  response_timestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (request_id);
```

## Build

```bash
cargo build --release
```

## Run

```bash
ENVOY_DYNAMIC_MODULES_SEARCH_PATH=target/release \
RUST_LOG=info \
envoy -c envoy-config.yaml
```

## Test

```bash
curl -v http://localhost:10000/get
```

Check logs for UUID and Spanner commits. Verify in Spanner console.
