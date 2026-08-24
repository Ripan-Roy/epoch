# Source connectors

**Development boundary:** beta-readiness branch

Epoch automatically ingests HTTP/CloudEvents batches, immutable objects,
PostgreSQL logical replication, MySQL row binlogs, and Kafka records into the
Event Bus that owns the connector. Every adapter feeds one leader-owned,
record-before-checkpoint pipeline. A source position advances only after each
record is durably applied or durably routed to connector error history.

| Kind | Input | Durable source position | Upstream acknowledgement |
|---|---|---|---|
| `http`, `cloud_event_bus` | Bounded JSON batch | Source-owned opaque cursor | None |
| `s3_compatible`, `azure_blob`, `azure_data_lake`, `gcs` | Immutable CloudEvents objects | Key plus version/ETag/size | None |
| `postgres_cdc` | Raw `pgoutput` changes | Commit LSN | Applied-LSN feedback after the Epoch checkpoint |
| `my_sql_cdc` | Raw version-4 row-binlog events | Binlog filename and next position | Reconnect from the Epoch checkpoint |
| `kafka` | Raw records or CloudEvents JSON | Next offset per topic/partition | Synchronous group commit after the Epoch checkpoint |

All connector configuration values are strings. Secret bytes never belong in
replicated connector configuration.

## Shared connector shape

Commit an `upsert_connector` integration operation through the regional Event
Bus API or any Go, Java, or Python SDK:

```json
{
  "kind": "upsert_connector",
  "spec": {
    "name": "orders-source",
    "kind": "postgres_cdc",
    "direction": "source",
    "secret_refs": ["orders-postgres"],
    "outbound_allowlist": ["orders-db.internal.example"],
    "identity": "orders-source-reader",
    "config": {
      "host": "orders-db.internal.example",
      "port": "5432",
      "database": "orders",
      "user": "epoch_replication",
      "slot": "epoch_orders",
      "publication": "epoch_orders",
      "tls_mode": "verify_full"
    }
  }
}
```

The connector must be active and its direction must be `source` or
`bidirectional`. Only the current Bus tablet leader opens the source.
Leadership loss, pause, deletion, or a missing materialized route closes
PostgreSQL and Kafka sessions, so an old leader cannot retain a slot or
consumer-group assignment.

### Apply and observe with an SDK

Parse the JSON operation above into the SDK's ordinary integration document,
use one caller-owned mutation identity, and then perform a linearizable state
read. The same operation and result shape is used by all three SDKs:

```go
receipt, err := bus.ApplyIntegration(ctx, "events", 0, "connector-orders-v1", operation)
if err != nil { return err }
state, err := bus.IntegrationState(ctx, "events", 0)
```

```java
JsonNode receipt = bus.applyIntegration(
    "events", 0, "connector-orders-v1", operationNode);
JsonNode state = bus.integrationState("events", 0);
```

```python
receipt = bus.apply_integration(
    "events", 0, "connector-orders-v1", operation)
state = bus.integration_state("events", 0)
```

The upsert creates an active connector. Pause it without deleting its durable
checkpoint by applying a new identity with
`{"kind":"set_connector_status","name":"orders-source","status":"paused"}`;
resume with `status:"active"`. Inspect the named entry under `connectors` in
`state` for its exact checkpoint, batch history, errors, secret references, and
status. The complete runnable client construction, regional scope, TLS, and
error-handling examples are in the
[regional Event Bus SDK guide](REGIONAL_EVENT_BUS_SDK.md), which is published
on the documentation site.

## Node-local credentials

Set `EPOCH_REGIONAL_MANAGED_TARGET_SECRETS_PATH` to a read-only JSON file. The
same bounded store supports HTTP authentication and typed connector
credentials:

```json
{
  "format_version": 1,
  "secrets": [
    {
      "kind": "connector_credentials",
      "reference": "orders-postgres",
      "values": {
        "username": "epoch_replication",
        "password": "replace-at-deploy-time"
      }
    }
  ]
}
```

One credential entry contains 1–64 named properties. The file accepts at most
1,024 entries and 1 MiB total. Debug/status output includes references only;
values are redacted. Wrong secret kinds and ambiguous references fail closed.

## HTTP and CloudEvents polling

| Property | Required | Contract |
|---|---|---|
| `source_url` | yes | Absolute safe HTTP(S) URL whose host is allowlisted |
| `start_position` | no | Initial opaque cursor; defaults to `0` |
| `poll_timeout_ms` | no | 1–30,000; defaults to 5,000 |

Epoch sends the connector identity and exact cursor:

```http
GET /events HTTP/1.1
Accept: application/json
Epoch-Connector-Identity: orders-source-reader
Epoch-Connector-Position: cursor-10
Authorization: Bearer <node-local-secret>
```

Return `204 No Content` or a strict response no larger than 4 MiB:

```json
{
  "batch_id": "orders-11",
  "source_from": "cursor-10",
  "source_to": "cursor-11",
  "events": [{
    "id": "order-11",
    "source": "urn:orders",
    "type": "order.created",
    "time_ms": 11,
    "payload": { "order_id": 11 }
  }]
}
```

The batch contains 1–1,000 unique valid events. `source_from` must equal the
sent position and `source_to` must differ. Unknown fields, cursor gaps,
duplicate IDs, invalid envelopes, oversized responses, unsafe destinations,
redirects, ambient proxies, and ambiguous authentication fail closed.

## Object storage

| Property | Default | Contract |
|---|---|---|
| `prefix` | root | Optional object-key prefix |
| `format` | `cloudevents_jsonl` | `cloudevents_json`, `cloudevents_json_array`, or `cloudevents_jsonl` |
| `max_batch_objects` | `8` | 1–64 objects |
| `max_object_bytes` | `4194304` | 1 byte–16 MiB |
| `anonymous` | `false` | Cannot be combined with credentials |
| `endpoint` | provider endpoint | Optional safe, allowlisted endpoint |

| Kind | Configuration | Credential values |
|---|---|---|
| `s3_compatible` | `bucket`; optional `region`, `virtual_hosted_style`, `endpoint` | `access_key_id`, `secret_access_key`; optional `session_token` |
| `azure_blob`, `azure_data_lake` | `account`, `container`; optional `endpoint` | `access_key`; or `bearer_token`; or `client_id`, `client_secret`, `tenant_id` |
| `gcs` | `bucket`; optional `endpoint` | `service_account_json` or `bearer_token` |

Listing scans at most 10,000 keys. A batch reads at most 16 MiB and 1,000
records, in lexical key order. Reads condition on the listed version/ETag. A
checkpointed key that later resolves to a different version, ETag, or size is
rejected as an overwrite. Deletion is allowed because the checkpoint proves
the object was consumed. Malformed or oversized objects become stable error
records so one bad immutable file does not wedge the prefix.

## PostgreSQL CDC

Required configuration is `host`, `database`, `user`, `slot`, and
`publication`. `port` defaults to 5432. `poll_timeout_ms` defaults to 250 and
is capped at 5,000. `max_transaction_bytes` defaults to 4 MiB and is capped at
16 MiB.

TLS hostname and certificate verification is the default. `ca_pem_path` adds a
CA. `client_cert_pem_path` and `client_key_pem_path` enable mTLS and must occur
together. `tls_mode=disable` is accepted only for explicitly enabled loopback
development. Credentials require `password`; `username` may override the
non-secret configured user.

Epoch requests logical replication using `pgoutput`. It buffers one bounded
transaction and emits raw `io.epoch.postgres.pgoutput.v1` events with base64
data, XID, WAL positions, and commit LSN. No checkpoint is emitted before
`Commit`. An oversized transaction produces one durable error at commit.
PostgreSQL receives applied-LSN feedback only after the Event Bus checkpoint.
The operator creates the publication and slot and monitors retained WAL while
a connector is paused.

## MySQL CDC

Required configuration is `host`, `database`, `user`, and `start_file`.
`port` defaults to 3306, `start_binlog_position` defaults to 4, and `server_id`
defaults to an Epoch-owned high ID. `max_transaction_bytes` has the same
4–16 MiB bounds as PostgreSQL.

TLS verification is the default. CA and client-certificate paths use the
PostgreSQL names; `tls_mode=disable` is loopback-development only. Credentials
require `password` and may include `username`.

The server enables version-4 row binlogs and grants replication privileges.
Epoch handles rotation, GTID/anonymous-GTID starts, `BEGIN`, `XID`, and
`COMMIT` boundaries. It emits raw `io.epoch.mysql.binlog.v1` base64 events and
checkpoints only a complete transaction. A stream ending mid-transaction
produces no checkpoint, so the next pass replays from the prior exact file and
position.

## Kafka

| Property | Contract |
|---|---|
| `brokers` | Comma-separated, allowlisted `host:port` entries |
| `group_id` | Consumer group owned by this connector |
| `topics` | 1–256 comma-separated literal topic names |
| `security_protocol` | `ssl` (default), `sasl_ssl`, or loopback-only `plaintext` |
| `sasl_mechanism` | `PLAIN`, `SCRAM-SHA-256`, or default `SCRAM-SHA-512` |
| `auto_offset_reset` | `earliest` (default) or `latest`, used only without an Epoch cursor |
| `format` | `raw` (default) or `cloudevents_json` |

Optional TLS paths are `ssl_ca_location`, `ssl_certificate_location`, and
`ssl_key_location`. SASL credentials require `username` and `password`.
`poll_timeout_ms` is 1–5,000. `max_batch_messages` is 1–1,000 and defaults to
256. `max_batch_bytes` defaults to 4 MiB and is capped at 16 MiB.

Automatic Kafka offset storage and commit are disabled; isolation is
`read_committed`. On every assignment generation, Epoch seeks assigned
partitions to its replicated cursor before accepting data. Event identity
includes source, topic, partition, and offset. Malformed CloudEvents and
oversized records become durable errors. After the replicated batch checkpoint,
Epoch synchronously commits next offsets to the group. If that commit fails,
the Epoch cursor remains authoritative and the next pass reconciles without
skipping data.

## Commit, crash, and replay ordering

For every adapter Epoch:

1. confirms the current non-fail-stopped Raft leadership term;
2. derives a stable proposal identity from connector, batch, and record index;
3. commits or resolves each Event Bus publish;
4. durably records every applied or error-routed result;
5. commits the batch receipt and exact `source_to` checkpoint;
6. only then acknowledges PostgreSQL or Kafka upstream.

A crash before step 5 replays the same stable identities. A crash between steps
5 and 6 reconciles upstream acknowledgement from the durable Epoch cursor. This
is at-least-once acquisition with duplicate-safe Event Bus admission for stable
source identities. Epoch does not claim exactly-once effects in an external
downstream system.

## Observe and verify

`GET /experimental/v1/regional/topology` reports source passes, connectors,
batches, applied records, error-routed records, checkpoints, errors, and the
last bounded error. Event Bus `integration/state` exposes the replicated
checkpoint, receipts, and bounded record-error history.

Fast deterministic tests:

```bash
cargo test -p epoch-node source_adapters --lib
```

Pinned live conformance:

```bash
docker compose -f deploy/compose/docker-compose.connectors.yml \
  up --detach --wait minio postgres mysql kafka
docker compose -f deploy/compose/docker-compose.connectors.yml \
  run --rm minio-init
cargo test -p epoch-node source_adapters --lib -- \
  --ignored --nocapture --test-threads=1
docker compose -f deploy/compose/docker-compose.connectors.yml \
  down --volumes --remove-orphans
```

CI runs the same MinIO/S3, PostgreSQL, MySQL, and Kafka contracts in the
`Source connector conformance` job. Live Azure and GCS emulator campaigns,
cloud IAM/workload identity, load/soak, and crash-at-every-network-boundary
certification remain production gates; beta implementation does not imply
those production claims.
