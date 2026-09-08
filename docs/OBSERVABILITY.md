# Observability and operational diagnostics

Epoch exposes a vendor-neutral production-observability baseline across the
Rust data plane, Go control plane, and Redis/Kafka/AMQP compatibility gateway.
Metrics are Prometheus text, logs can be structured JSON, and traces use W3C
Trace Context with OTLP/HTTP export. The metrics listeners are separate from
the public APIs and should remain reachable only by the monitoring network.

## Runtime endpoints

| Process | Default listener | Endpoints | Configuration |
|---|---:|---|---|
| `epoch-node` | `127.0.0.1:7602` | `/healthz`, `/metrics`, `/v1/diagnostics/latency` | `EPOCH_METRICS_LISTEN` |
| `epoch-control` | `127.0.0.1:9090` | `/metrics` | `EPOCH_CONTROL_METRICS_ADDR` |
| `epoch-compat` | `127.0.0.1:9100` | `/metrics` | `EPOCH_COMPAT_METRICS_LISTEN` |

The public data and control listeners carry `X-Request-ID` on responses and
preserve one valid caller-supplied value. `EPOCH_JSON_LOGS=true` enables JSON
for Rust processes; the Go control plane always emits structured `slog` JSON.
HTTP completion logs include stable request, event, trace, profile, operation,
status, and duration fields. Compatibility spans use closed protocol and
operation names, and connection failures use structured warnings. Tokens and
payloads are not completion-log fields; resource names and raw tenant names are
not metric labels.

## Prometheus metrics

The principal stable metric families are:

- `epoch_http_requests_total` and
  `epoch_http_request_duration_seconds` for data-plane requests;
- `epoch_operation_stage_duration_p99_seconds` for the currently measured
  regional routing, replication, and storage stages;
- `epoch_control_http_requests_total`,
  `epoch_control_http_request_duration_seconds`,
  `epoch_control_reconciliations_total`, and
  `epoch_control_reconciliation_duration_seconds`;
- `epoch_compat_requests_total` and
  `epoch_compat_request_duration_seconds` for closed Redis, Kafka, and AMQP
  operation families;
- `epoch_observability_tenants`,
  `epoch_control_observability_tenants`, and the matching overflow counters.

HTTP routes are classified into a closed profile/operation vocabulary before
recording. The tenant label is a deterministic non-cryptographic fingerprint,
not an authorization or secrecy mechanism. Each Rust
process admits at most `EPOCH_OBSERVABILITY_MAX_TENANTS` exact fingerprints
(default 1,024); control uses
`EPOCH_CONTROL_OBSERVABILITY_MAX_TENANTS`. New tenants beyond the bound share
the `overflow` series and increment an explicit alertable counter.

Useful queries:

```promql
# Data-plane request rate by profile and outcome
sum by (profile, outcome) (rate(epoch_http_requests_total[5m]))

# Data-plane p99 by profile
histogram_quantile(
  0.99,
  sum by (le, profile) (rate(epoch_http_request_duration_seconds_bucket[5m]))
)

# Dominant measured server stage
max by (profile, stage) (epoch_operation_stage_duration_p99_seconds)

# Compatibility traffic by wire protocol and operation family
sum by (protocol, operation, outcome) (rate(epoch_compat_requests_total[5m]))
```

Import
[`deploy/observability/grafana-dashboard.json`](../deploy/observability/grafana-dashboard.json)
into Grafana. Load
[`deploy/observability/prometheus-rules.yaml`](../deploy/observability/prometheus-rules.yaml)
with Prometheus or a Prometheus-compatible ruler. Repository contract tests
parse both files, require the core signals, require actionable runbook links,
and reject customer-controlled labels in alert expressions.

## OpenTelemetry traces

Set the same OTLP/HTTP base endpoint on any process that should export traces:

```bash
export EPOCH_OTLP_ENDPOINT=http://127.0.0.1:4318
export EPOCH_JSON_LOGS=true
```

The endpoint must be HTTP(S) and cannot contain credentials, a query, or a
fragment. Epoch batches spans and attempts a bounded flush at shutdown. The
data plane extracts `traceparent` from HTTP ingress; the control plane extracts
it from HTTP/gRPC and injects it into regional calls; the compatibility gateway
creates one span per closed Redis/Kafka/AMQP operation and injects the resulting
context into its native HTTP call.

The supplied
[`deploy/observability/otel-collector.yaml`](../deploy/observability/otel-collector.yaml)
accepts OTLP gRPC/HTTP locally, applies memory and batch limits, and forwards to
`OTEL_EXPORTER_OTLP_ENDPOINT`. Supply collector authentication through its
deployment secret mechanism; never place it in the Epoch endpoint URL.

### SDK propagation

Go, Java, and Python event envelopes expose an optional W3C `traceparent`
field, so publish-to-delivery correlation survives the durable event. For HTTP
client spans, configure the ecosystem OpenTelemetry HTTP instrumentation to
send the current `traceparent` header. Epoch accepts only the canonical W3C
format and starts a safe new root when an inbound value is invalid. Application
code should not invent trace IDs or reuse a parent across unrelated work.
Versioned snapshots written by older prereleases may retain an opaque invalid
value so recovery remains byte-compatible; Epoch never forwards that value,
and every new mutation rejects it.

## “Why is it slow?”

The console resource table exposes **Why slow?** for supported regional
profiles. It calls the authenticated control endpoint:

```text
GET /v1/observability/latency?organization=acme&project=shop&environment=prod&namespace=core&profile=stream
Authorization: Bearer …
```

Control enforces the exact requested tenant scope and queries the internal
data-plane diagnostic endpoints with bounded failover and a 1 MiB response
ceiling. The result contains the measured p99, sample count, source region, the
dominant stage, and one bounded recommendation. If no successful measurement
exists, the API returns `not_found` and the console says so; it does not guess.

## Kubernetes and Compose

The operator exposes node metrics at `7602` on the headless peer Service and
control metrics at `9090` on the internal control Service. The public Epoch
Service does not expose metrics. Service discovery annotations identify the
metrics port. `spec.observability.otlpEndpoint` injects the validated collector
endpoint into both process types, and control receives every node's internal
diagnostic address.

The standalone and regional Compose definitions publish loopback metrics
ports. Do not bind them publicly without an authenticated reverse proxy or a
network policy.

## Runbook: availability

1. Confirm the target and Service endpoints exist.
2. Query the internal `/healthz` and public health endpoint independently.
3. Check leader/quorum evidence before restarting a voter.
4. Preserve logs and trace IDs for the affected window.

## Runbook: error rate

Group errors by bounded profile, operation, and outcome. Use a request ID to
join the structured log to a trace. Client errors require contract/input
inspection; server errors require quorum, storage, and target evidence before a
retry policy is changed.

## Runbook: tail latency

Open **Why slow?** or compare the stage p99 series. Replication suggests
voter/network health, storage suggests WAL/snapshot pressure, and routing
suggests discovery or redirect churn. Quota, hot-partition, target, and client
categories are reserved by the closed contract but are not emitted by this
increment; Epoch does not infer them without a measurement.

## Runbook: control plane

Inspect reconciliation errors and durations, then verify durable registry
access and each configured regional endpoint. Do not repeatedly apply desired
state until the recorded error is understood; retries retain exact operation
identity.

## Runbook: compatibility gateways

Group failures by protocol and closed operation family. Run `epoch-compat scan`
against the observed command/API manifest before assuming an unsupported wire
operation is a runtime outage. Native HTTP request IDs and traces identify the
translated regional call.

## Runbook: cardinality

An overflow alert means more tenant scopes were observed than the configured
series budget. Confirm it is legitimate tenant growth, then raise the bounded
limit only with a Prometheus capacity review. Epoch never promotes resource
names, cache keys, topics, queues, consumer IDs, or payload fields into metric
labels.

## Current evidence boundary

This baseline provides process/request/protocol rates and latency, measured
stage attribution, structured correlation, trace propagation/export,
deployment discovery, a dashboard, alerts, and the console diagnostic. Detailed
state gauges for every profile item in PRD section 14.1 (for example cache
fragmentation, every queue age, and connector batch-size distributions) remain
separate profile-instrumentation work and must not be inferred from the current
dashboard. Production SLO claims also require the published reference-hardware
load and fault campaigns.
