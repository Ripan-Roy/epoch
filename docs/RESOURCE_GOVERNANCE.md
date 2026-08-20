# Resource Governance

Epoch requires every newly managed regional resource to declare bounded,
non-secret governance metadata. The metadata is part of desired state, is
generation-fenced, is replicated into the Rust regional catalog, and remains
queryable after control-plane or data-plane recovery.

## Contract

```json
{
  "owner": "team:platform",
  "cost_center": "cc-1042",
  "classification": "confidential",
  "tags": {
    "profile": "stream",
    "service": "orders"
  }
}
```

- `owner` and `cost_center` are required, trimmed, lower-cased identifiers.
- `classification` is exactly `public`, `internal`, `confidential`, or
  `restricted`.
- tags are exact-match metadata: at most 32 entries, keys at most 63 bytes,
  values at most 256 bytes, and keys are canonicalized to lower case.
- the `epoch.io/` tag prefix is reserved for future service-owned metadata.
- environment is not duplicated. The environment in the fully qualified
  resource name is authoritative for authorization and placement.

Owner, cost center, classification, and tags are metadata, never credential or
secret fields. Updating them requires an exact expected generation and creates
a new desired generation. A stale update is rejected.

Valid legacy catalog and durable-registry records without governance remain
readable. New resources entering through the managed regional API must provide
the complete governance object.

## Create a governed resource

```shell
curl --fail-with-body --request PUT http://127.0.0.1:8080/v1/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --header 'content-type: application/json' \
  --data '{
    "request_token":"docs-create-governed-orders-v1",
    "expected_generation":0,
    "resource":{
      "organization":"acme","project":"shop","environment":"dev",
      "namespace":"core","kind":"stream","name":"orders",
      "governance":{
        "owner":"team:platform","cost_center":"cc-1042",
        "classification":"confidential",
        "tags":{"profile":"stream","service":"orders"}
      },
      "spec":{"shard_count":3,"replica_count":3,"placement":{
        "allowed_regions":["ap-south"],"minimum_zones":3,
        "required_node_class":"general-purpose"
      }}
    }
  }'
```

The Go management API canonicalizes and durably records the desired value. Its
reconciler forwards the same value to the Rust catalog, where canonical command
and snapshot version 3 preserve it through quorum replication and reopen.

## Filter inventory and attribute cost drivers

```shell
curl --fail --get http://127.0.0.1:8080/v1/regional/resources \
  --header 'authorization: Bearer epoch-dev-admin-v1' \
  --data-urlencode 'owner=team:platform' \
  --data-urlencode 'cost_center=cc-1042' \
  --data-urlencode 'classification=confidential' \
  --data-urlencode 'tag=service=orders' \
  --data-urlencode 'tag=profile=stream'
```

All supplied filters use AND semantics. Tags are repeated `tag=key=value`
parameters and match exact canonical keys and exact trimmed values. Invalid,
duplicate, reserved, or oversized filters fail closed.

The browser BFF returns `cost_attribution` after tenant authorization and
filtering. Rows are deterministically ordered by cost center and classification
and contain resource and desired-shard counts. These are explainable allocation
drivers, not currency, metering, invoices, or a billing ledger.

The console exposes the same filters, governance columns, and attribution
summary. It continues to read only the Go BFF and does not contact Rust data
nodes or retain the session bearer outside session storage.

## Verification and boundaries

Unit and contract tests cover normalization, duplicate canonical tag rejection,
reserved namespaces, bounded input, generation-fenced ownership transfer,
gRPC/HTTP round trips, exact filtering, post-authorization aggregation, and
legacy reads. The regional container campaign proves the same governance value
in the Go inventory and Rust catalog before and after Go `SIGKILL`, leader
replacement, and all-node same-volume reopen.

This capability completes the metadata and cost-driver scope of GOV-005. It
does not implement ABAC rules derived from those fields, organization policy,
currency rates, usage metering, invoices, immutable audit export, payload
redaction, residency, legal hold, or recoverable deletion.
