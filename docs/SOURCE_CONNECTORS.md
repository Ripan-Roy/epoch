# HTTP source connectors

**Release boundary:** `v0.1.0-alpha.10`

Epoch can ingest a bounded batch from an HTTP or CloudEvents source connector
into the Event Bus that owns the connector resource. The worker runs only on
the current tablet leader. It uses the same external-secret, DNS-pinned,
no-proxy, no-redirect, timeout-bounded HTTP boundary as managed targets.

This release does not claim PostgreSQL/MySQL CDC, Kafka consumer-group, object
storage, private-network, or marketplace certification. Those connector kinds
remain modeled but the built-in source executor rejects them explicitly.

## Configure a source

Commit this integration operation through `apply_integration` in any regional
Go, Java, or Python Event Bus client:

```json
{
  "kind": "upsert_connector",
  "spec": {
    "name": "orders-source",
    "kind": "http",
    "direction": "source",
    "secret_refs": ["orders-reader"],
    "outbound_allowlist": ["source.example.com"],
    "identity": "orders-source-reader",
    "config": {
      "source_url": "https://source.example.com/events",
      "start_position": "cursor-0",
      "poll_timeout_ms": "5000"
    }
  }
}
```

`source_url` is required. `start_position` defaults to `0` and is used only
before the first committed checkpoint. `poll_timeout_ms` defaults to 5 seconds
and is bounded to 30 seconds. At most one external secret reference is allowed.
Plaintext secret, password, and token configuration keys are rejected by the
connector registry.

For local development only, set
`EPOCH_REGIONAL_MANAGED_TARGET_ALLOW_HTTP_LOOPBACK=true`. Production mode
requires HTTPS with a public DNS/IP destination and an exact hostname in
`outbound_allowlist`.

## Poll contract

Epoch sends:

```http
GET /events HTTP/1.1
Accept: application/json
Epoch-Connector-Identity: orders-source-reader
Epoch-Connector-Position: cursor-10
Authorization: Bearer <external-secret-value>
```

Return `204 No Content` when no batch is ready. Return `200 OK` with a strict
body no larger than 4 MiB:

```json
{
  "batch_id": "orders-11",
  "source_from": "cursor-10",
  "source_to": "cursor-11",
  "events": [
    {
      "id": "order-11",
      "source": "urn:orders",
      "type": "order.created",
      "time_ms": 11,
      "payload": { "order_id": 11 }
    }
  ]
}
```

The batch must contain 1–1,000 valid events with unique IDs. `source_from`
must equal the exact position Epoch sent and `source_to` must advance it.
Unknown fields, gaps, duplicate event IDs, invalid event envelopes, oversized
responses, unsafe destinations, and ambiguous authentication fail closed.

## Commit and crash ordering

For each record, Epoch derives a stable proposal identity from connector,
batch, and record index. It then:

1. confirms it still owns the current Raft leadership term;
2. commits or resolves the exact Event Bus publish proposal;
3. records an applied or non-retryable error-routed result;
4. repeats for every record in order;
5. commits the batch result and `source_to` checkpoint through consensus.

A consensus/leadership failure stops the batch without checkpointing. A crash
after some publishes but before the checkpoint causes the source to return the
same range again; the stable proposal identities resolve the prior committed
outcomes rather than appending duplicate events. A cursor gap never silently
skips data. A rejected event is recorded in the connector error history before
the batch checkpoint advances.

This is at-least-once source polling with effectively-once Event Bus admission
for a stable batch identity. Epoch does not claim exactly-once side effects in
the external source.

## Observe and recover

`GET /experimental/v1/regional/topology` reports
`source_connector_delivery` counters for passes, connectors, batches, applied
events, error-routed events, checkpoints, errors, and last error. Event Bus
`integration/state` exposes the replicated checkpoint, batch receipt, and
record error history.

The real three-process suite proves exact cursor headers, publish-before-
checkpoint ordering, all-voter convergence, full process stop/restart, and
checkpoint/state reopen. Run it with:

```bash
cargo test -p epoch-node --test regional_process \
  regional_processes_fail_over_reopen_and_converge -- --test-threads=1
```
