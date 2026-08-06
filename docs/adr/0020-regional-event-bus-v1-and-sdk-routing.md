# ADR-0020: Regional Event Bus v1 and SDK routing

- Status: Accepted
- Date: 2026-08-06
- Owners: Rust regional ingress and tablet runtime; Go, Java, and Python SDKs
- Related: BUS-001–005, BUS-011, DX-001–002, MT-10, G4, ADR-0011, ADR-0013, ADR-0017–0019

## Context

Epoch already has one deterministic Event Bus tablet state machine. It commits subscription route plans, publishes CloudEvents-shaped envelopes, optionally archives events, and records target-isolated delivery intent with fenced acquisition, acknowledgement, failure, retry, and dead-letter state. The regional materializer hosts that tablet behind the experimental generic resource router.

Applications do not yet have a stable, fully qualified Event Bus route or a complete native client. The standalone SDK methods use a different in-process API and do not expose the replicated delivery ledger. Sending regional Bus traffic through the Go control plane or creating a second SDK-side state model would split authority and weaken recovery guarantees.

## Decision

### One native regional route

The versioned resource root is:

`/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}`

`GET` on the root performs authenticated leader discovery. Operations below the root adapt exactly to the existing Event Bus tablet router:

- `POST mutations`
- `GET mutations/{proposal_id}`
- `POST archive/replay`
- `POST deliveries/query`
- `GET status`

The adapter preserves the resource-generation and tablet-epoch fences, current-term write admission, and the existing strict tablet request/response contracts. It does not proxy data through Go and does not create another store.

### Authorization

Discovery requires `route.read`. Mutation submission requires `data.write`. Mutation lookup and status use `data.read` because they are `GET` operations. `archive/replay` and `deliveries/query` are query-shaped `POST` operations and require `data.read`; all other operation `POST`s require `data.write`. The organization, project, environment, and namespace are decoded from the exact route and matched deny-by-default.

### Reads and retries

Archive replay, delivery query, status, and mutation lookup request linearizable reads. The regional router performs a leader ReadIndex barrier and never silently downgrades to stale local state.

Every SDK performs bounded discovery across configured endpoints and at most one rediscovery after a retryable transport, leader, fence, route, or read-barrier failure. A retry reuses the caller's exact idempotency key and operation body. It never invents a second mutation identity.

### Regional Event Bus lifecycle

The Go, Java, and Python clients expose the complete current replicated lifecycle:

1. upsert or remove a typed subscription;
2. publish one strict event envelope;
3. acquire bounded delivery leases for one subscription and dispatcher epoch;
4. acknowledge or fail one fenced lease;
5. explicitly maintain due retries and expired leases;
6. resolve a mutation by proposal ID;
7. replay a bounded archive time range with an optional filter;
8. query bounded delivery records by subscription and state;
9. observe linearizable tablet status.

Browser-unsafe unsigned integers are encoded as decimal strings. SDK validation rejects empty identities, invalid bounds, unsupported delivery states or strategies, malformed typed targets, and invalid retry-policy relationships before network I/O.

### Delivery intent is enabled for regional Bus tablets

Regional materialization enables the existing replicated delivery outbox. This makes pull dispatch and settlement available through the native regional API. The same Event Bus state machine still owns the route plan, archive, and delivery ledger; no executor runs inside consensus.

## Explicit non-claims

This increment does not claim:

- an HTTP, webhook, Queue, or Stream target executor;
- webhook signing, secret rotation, OAuth, or replay protection;
- push streaming, long polling, dispatcher sessions, or automatic lease renewal;
- archive search indexes or unbounded replay/query;
- schema validation, enrichment, connectors, MQTT, or geo routing;
- exactly-once external side effects.

The delivery ledger proves replicated intent and fenced settlement. A worker must perform the external side effect using its own idempotency contract before acknowledging Epoch.

## Consequences

- All four P0 profile tablets have stable fully qualified regional routing and official Go, Java, and Python client boundaries.
- Existing standalone Bus calls remain source compatible; shared subscription models gain optional delivery-policy fields.
- Regional Bus resources begin recording delivery intent for matching subscriptions.
- Real recovery evidence must include a leader replacement, SDK route-plan mutation, publish replay, acquisition and settlement, archive/delivery reads, old-voter catch-up, and all-voter reopen.

## Verification

- red/green Rust adapter and authorization tests;
- red/green Go, Java, and Python transport-contract tests;
- strict local unit, lint, type, audit, and build gates;
- a three-node container campaign using the exact Python client after active-leader loss;
- exact compilable SDK examples embedded in the docs-only GitHub Pages bundle;
- protected pull-request and exact-main CI followed by main-only Pages deployment.
