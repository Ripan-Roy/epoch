# ADR-0037: Replicated Event integration platform and leader-owned delivery

- Status: Accepted for `v0.1.0-alpha.9`
- Date: 2026-08-23
- Requirements: `BUS-001` through `BUS-015`

## Decision

Epoch completes the native Event Bus alpha as one deterministic replicated
state machine plus leader-owned side-effect executors. Routing, schemas,
validation policies, enrichment data, MQTT state, connector resources and
checkpoints, endpoint observations, catalog entries, function resources,
archive retention, delivery rate limits, dead-letter retention, and redrive
are committed through the Event Bus tablet. Network and target I/O never run
inside that state machine.

Each successful tablet mutation must remain encodable as a bounded native
snapshot before the staged state is published. Integration state has a 2-MiB
admission ceiling inside the 4-MiB Event Bus image, and restore revalidates
registry identities, revision sequences, cross-references, limits, endpoint
URLs, checkpoints, receipts, MQTT topics/sessions, and the recovery digest.
State-dependent invalid/capacity operations commit typed rejections without
publishing their candidate state.

## Delivery ownership and ordering

Only the current source Event Bus leader performs target I/O. A worker selects
an eligible delivery and first commits an exact lease bound to the current
tablet/leader fence. It then resolves the immutable replicated target metadata,
executes one bounded attempt with a stable delivery idempotency key, and commits
one of acknowledgement, retryable failure, or terminal rejection.

For connector targets, a successful external batch is not settled at the
source until the connector batch outcome and checkpoint commit. An exact replay
of the same logical batch returns its original receipt even if the worker's
later committed timestamp differs. This closes the crash window between target
success and source settlement without claiming an atomic distributed
transaction or exactly-once behavior from an arbitrary external receiver.

## HTTP, CloudEvents, and credentials

API destinations and function resources support CloudEvents 1.0 binary or
structured JSON requests. API destinations may use an API-key reference, a
bearer-token reference, or OAuth 2 client credentials. OAuth tokens are fetched
through the same safe client and cached only in process memory for their
bounded lifetime. Secrets are supplied by a strict size-bounded external file;
replicated state stores references, never secret values.

Every outbound URL is absolute and credential-free, its host must match the
resource allowlist, redirects and ambient proxies are disabled, DNS is resolved
per attempt, and the selected public address is pinned. Loopback HTTP exists
only behind an explicit development flag used by real-process tests. Endpoint
pools choose deterministic healthy routes; an actual egress failure commits an
unhealthy observation before a later attempt can fail over. Authentication or
configuration errors do not poison endpoint health.

## Schemas, transforms, and enrichment

The replicated registry stores bounded Avro, JSON Schema, and Protobuf source
definitions with monotonic revisions and structural compatibility checks.
Validation policies bind event-type patterns to exact schema revisions at the
producer and/or broker boundary. The alpha validator enforces Epoch's declared
field model; it is not a substitute for every official format compiler.

Transforms support bounded projection, rename, constants, templates, headers,
and replicated lookup enrichment. Enrichment is synchronous and deterministic:
network access is forbidden, input/output sizes and operation counts are
bounded, and a required missing lookup fails closed.

## MQTT, catalog, endpoints, functions, and connectors

MQTT state defines persistent sessions, expiry, retained messages, QoS mapping,
wildcard matching, and deterministic shared-subscription cursors. This is the
replicated semantic core for a future gateway; alpha.9 does not expose an MQTT
5 wire listener or claim protocol conformance.

Catalog entries retain bounded owner, schema reference, source/consumer
lineage, classification, and sample payload metadata. Endpoint observations
provide deterministic priority/region/URL routing. Function definitions and
connectors retain identity, revision/status, outbound allowlists, secret
references, configuration, checkpoints, partial record errors, replay intent,
and secret-rotation versions. Target and bidirectional connector destinations
execute through the managed worker. Automatic polling of external source
connectors remains outside this release.

## Compatibility and non-claims

- Legacy Event Bus snapshot versions remain readable when their newer state is
  absent; integration and archive-retention images use their additive versions.
- Existing signed webhook and pinned Epoch Queue/Stream target contracts remain
  unchanged.
- Go, Java, and Python use the same regional route, fence, idempotency, long-
  poll, maintenance, and integration-operation contract.
- Secret-manager integration, hot reload, private destinations, OAuth variants
  beyond client credentials, official schema/MQTT/CloudEvents conformance,
  automatic source polling, health-probe restoration, production performance,
  exhaustive fault injection, and formal security review remain production or
  compatibility gates.
