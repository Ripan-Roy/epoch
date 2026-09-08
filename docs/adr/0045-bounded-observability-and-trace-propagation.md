# ADR-0045: Bounded observability and trace propagation

- Status: Accepted
- Date: 2026-09-08
- Owners: Runtime, control plane, developer experience

## Context

Epoch crosses Rust HTTP and protocol gateways, a Go management boundary, and
durable profile state. Raw resource or tenant labels would make Prometheus
cardinality customer-controlled. Independent request IDs or traces at each hop
would make an incident impossible to follow, while exposing diagnostics on the
public API would widen the authenticated product surface.

## Decision

1. Each process owns an internal metrics listener separate from its public
   traffic listener. Kubernetes exposes only cluster-internal metrics Services.
2. Metric dimensions use closed profile, operation, outcome, stage, and
   protocol vocabularies. Tenant scope is a deterministic non-cryptographic fingerprint
   admitted through a fixed per-process budget; excess tenants share an
   explicit overflow series.
3. Public HTTP requests preserve one safe `X-Request-ID`. Logs carry the stable
   request/event ID and current trace ID but never bearer credentials or
   payloads.
4. W3C Trace Context is the propagation contract. Data ingress extracts it,
   Go HTTP/gRPC and regional clients propagate it, and Redis/Kafka/AMQP gateway
   operations create spans that inject into native HTTP calls.
5. OTLP/HTTP is the optional trace export boundary. Endpoint validation rejects
   credentials, queries, fragments, non-HTTP schemes, and malformed URLs.
6. Tail-latency diagnosis uses measured stage samples only. The authenticated
   control plane enforces exact tenant scope before reading a bounded internal
   node response. No-sample results remain unknown instead of guessed.
7. Dashboard, alert, and collector templates are repository-tested contracts.

## Consequences

Operators can use Prometheus, Grafana, and any OTLP-compatible backend without
coupling Epoch to one vendor. Metrics remain safe under resource-name and key
growth. Traces cross management, regional, and compatibility boundaries. The
fingerprint is suitable for bounded correlation, not secrecy, display, or authorization; raw
identity remains available only after an authenticated resource lookup.

Profile-specific state gauges and production SLO evidence still require their
own bounded instrumentation and load/fault campaigns. This decision does not
turn an unmeasured state into a health claim.
