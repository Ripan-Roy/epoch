# ADR-0030: Leader-owned signed webhook delivery

- **Status:** Accepted
- **Date:** 2026-08-20
- **Owners:** Rust data plane and SDKs
- **Requirements:** BUS-003, BUS-004, BUS-005, BUS-011, DX-001, DX-002

## Context

The Event Bus tablet already replicated subscription intent, transformed
CloudEvents-shaped envelopes, delivery policy, leases, attempts,
acknowledgement, and dead-letter state. An external pull worker could settle
that ledger, but Epoch did not perform an HTTP side effect. Calling a webhook
from the replicated state machine would make consensus depend on nondeterministic
network I/O. Calling it before a lease commits can duplicate or lose work, and
treating an initially pending Raft proposal as complete can leave a committed
lease with no HTTP attempt.

Webhook URLs are also an egress-security boundary. DNS can change between
validation and connection, redirects and ambient proxies can escape a target
policy, and a signature without a bounded timestamp and durable replay identity
does not protect a receiver from replay.

## Decision

### Ownership and ordering

The regional Rust runtime owns one built-in webhook worker per node. The worker
is enabled only when a signing-key file is configured. On every bounded pass it
examines materialized Event Bus groups, and only the current non-fail-stopped
Raft leader may dispatch a candidate.

For each subscription the tablet exposes the oldest due signed HTTP/webhook
record that the existing acquire transition can select. The worker commits an
exact-delivery acquisition and waits for that proposal's committed receipt
before performing network I/O. It then commits exactly one acknowledgement,
retry failure, or terminal rejection with the returned opaque lease token.
Acquire and settlement identities are deterministic over the delivery ID and
attempt, so an exact retry resolves the original proposal rather than creating
another transition.

The ordering is therefore:

```text
publish and route commit
  -> exact lease acquisition commits
  -> signed HTTP request
  -> ack / retry / terminal rejection commits
```

Leadership loss or process failure can still cause another attempt after lease
expiry. The contract is at-least-once. Receivers must make the pair
`(epoch-delivery-id, epoch-delivery-attempt)` durable before applying business
side effects.

### HTTP and CloudEvents contract

The worker sends the event payload as the exact request body and maps the
supported envelope attributes to CloudEvents 1.0 binary-mode headers:

- `ce-specversion: 1.0`
- `ce-id`, `ce-source`, `ce-type`, and optional `ce-subject`
- the envelope `content-type` and optional `traceparent`
- `epoch-subscription`
- `epoch-delivery-id` and canonical positive `epoch-delivery-attempt`
- `epoch-signature-key-id`, `epoch-signature-timestamp`, and `epoch-signature`

Any 2xx response acknowledges the delivery. `429`, 5xx, connection, DNS, and
timeout failures enter the replicated retry policy. Other non-2xx responses,
unsafe targets, and invalid delivery metadata are terminal dead letters.
Redirects are disabled and response bodies are not consumed.

### Signature and replay contract

Version 1 uses HMAC-SHA-256. The worker signs this exact ASCII/UTF-8 value,
including the four newline separators:

```text
v1
<unix-timestamp-seconds>
<delivery-id>
<attempt>
<lowercase-hex-sha256-of-exact-body>
```

The header is `epoch-signature: v1=<lowercase-hex-hmac>`. Go, Java, and Python
verification helpers parse canonical decimal attempt/timestamp fields, enforce
a caller-selected positive timestamp tolerance, recompute the exact-body MAC,
compare it in constant time, and return the replay identity. They do not own the
receiver's replay store.

The shared test vector is:

```text
secret:      0123456789abcdef0123456789abcdef
body:        {"order_id":"one"}
timestamp:   1700000000
delivery ID: epoch.bus.delivery.v1.1.orders
attempt:     2
signature:   v1=866b035f5c00f59cc64a7caea8a4d16be04dd41966774cdfc336e7cf341d18d9
```

### Keys and rotation

Only a bounded key ID is replicated in subscription and delivery state. Secret
bytes remain in a strict external JSON file loaded independently by each node:

```json
{
  "format_version": 1,
  "keys": [
    {
      "id": "primary",
      "secret": "replace-with-at-least-32-byte-secret"
    }
  ]
}
```

The file permits 1–32 unique keys, is limited to 256 KiB, and requires each
secret to contain 32–4,096 bytes. Keeping old and new IDs during an overlap
window supports rotation; route updates select the key for new delivery
records. Key material is redacted from debug output. File hot reload and an
external secret manager remain later work.

### Egress policy

Production attempts require HTTPS. Plain HTTP is accepted only for an explicit
loopback development flag. URL user information and fragments are rejected.
Every domain is resolved on every attempt; all returned addresses must pass the
public-address policy, and those exact addresses are pinned into the request
client while retaining the hostname for TLS. Private, loopback, link-local,
multicast, unspecified, documentation, transition, benchmark, carrier-grade,
and protocol-only ranges are denied for IPv4 and IPv6. Mixed public/private DNS
answers fail closed. Redirect following and ambient proxy discovery are
disabled.

The complete DNS-plus-request timeout is capped by the remaining replicated
lease, and an already-expired lease emits no request. An operator
cannot configure this worker as a general private-network connector; an
explicit managed egress profile is future work.

### Compatibility

Unsigned v1 subscriptions, commands, and snapshots retain their prior bytes.
Signed targets, exact internal acquisition, and terminal rejection require
Event Bus command/snapshot format v2. Decoders accept both versions and reject
content mislabeled with the wrong minimum format.

## Consequences

- The storage transition remains deterministic and free of external I/O.
- A committed lease is observed before its HTTP request, closing the
  pending-proposal gap found by the real-process test.
- Secret rotation does not rewrite historical delivery records or replicate
  secret bytes.
- Receivers get native verification helpers but must implement durable replay
  suppression and business idempotency.
- Unsigned HTTP/webhook targets remain durable intent for external dispatchers;
  the built-in worker deliberately handles signed targets only.
- Queue/Stream targets remain outside this decision and are addressed by
  ADR-0031. Public push/long-poll, rate limiting, redrive/retention,
  OAuth/API-key destinations, private egress, and broad CloudEvents conformance
  remain open.

## Evidence

- deterministic Event Bus candidate, exact-acquire, v1/v2 compatibility,
  retry, rejection, and snapshot tests;
- RFC 4231 HMAC plus the shared Go/Java/Python signature vector;
- real HTTP receiver tests for exact body and headers;
- special-address, mixed-DNS, redirect, proxy, invalid-header, key-file, and
  secret-redaction tests; and
- a real three-process fixed-voter campaign that receives 503 then 204,
  observes attempts 1 and 2 with distinct signatures, converges the
  acknowledged attempt history on every voter, and verifies it again after all
  three processes reopen their existing storage.
