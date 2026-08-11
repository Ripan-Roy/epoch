# Regional Stream SDK

**Status:** Versioned partition-0 alpha

**Languages:** Go, Java, and Python

The regional Stream client is the application-facing path to a Stream tablet
replicated by the fixed three-voter runtime. It discovers the active Rust
leader, authenticates every request, carries delete/recreate and tablet fences,
and requests quorum-confirmed reads. It does not send application data through
the Go control plane.

See [ADR-0017](adr/0017-regional-stream-v1-and-sdk-routing.md) for the binding
decision, [ADR-0023](adr/0023-stream-retention-policies.md) for retention, and
[Regional runtime](REGIONAL_RUNTIME.md) for provisioning and operations.

## End-to-end flow

```text
application SDK
   |
   | 1. authenticated GET of the fully qualified shard
   v
configured Rust endpoints ---- selects accepts_writes=true
   |
   | 2. operation + observed generation/tablet epoch
   |    mutation also carries observed term + caller idempotency key
   v
current Stream tablet leader
   |
   | majority commit, local profile apply, typed receipt
   v
application
```

Discovery occurs before every operation. A leader or fence race triggers one
bounded rediscovery attempt only when the error is retryable. The mutation's
idempotency key never changes.

## Provision the local three-node region

Start the three Rust voters:

```shell
make compose-regional-up
```

Start `epoch-control` in another terminal:

```shell
EPOCH_CONTROL_REGIONAL_ENDPOINTS=http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663 \
EPOCH_CONTROL_STATE_PATH=.epoch/control/registry.db \
EPOCH_AUTH_POLICY_PATH=spec/auth/bootstrap-policy-v1.example.json \
EPOCH_CONTROL_REGIONAL_TOKEN=epoch-dev-control-v1 \
go run ./control/cmd/epoch-control
```

Apply the `acme/shop/dev/core` Stream named `orders` using the exact request in
[Apply one managed resource](REGIONAL_RUNTIME.md#apply-one-managed-resource).
The development admin token is `epoch-dev-admin-v1`. These checked-in
credentials are public fixtures and must never be reused outside a disposable
local environment.

## Constructors

| Language | Scope | Client |
|---|---|---|
| Go | `epoch.RegionalScope{Organization, Project, Environment, Namespace}` | `epoch.NewRegionalStreamClient(endpoints, token, scope, timeout)` |
| Java | `new RegionalScope(organization, project, environment, namespace)` | `new RegionalStreamClient(endpoints, token, scope, timeout)` |
| Python | `RegionalScope(organization, project, environment, namespace)` | `RegionalStreamClient(endpoints, token=..., scope=..., timeout=...)` |

Pass every known node endpoint. For the local Compose region those are
`http://127.0.0.1:18661`, `:18662`, and `:18663`. Custom transports are
available for tests in all three SDKs.

## Operations

| Semantics | Go | Java | Python |
|---|---|---|---|
| Single append | `Append` | `append` | `append` |
| Offset fetch | `Fetch` | `fetch` | `fetch` |
| Commit or reset checkpoint | `CommitOffset` | `commitOffset` | `commit_offset` |
| Observe checkpoint and lag | `Lag` | `lag` | `lag` |
| Fetch from checkpoint | `FetchGroup` | `fetchGroup` | `fetch_group` |
| Configure retention | `ConfigureRetention` | `configureRetention` | `configure_retention` |
| Commit idle maintenance | `MaintainRetention` | `maintainRetention` | `maintain_retention` |
| Observe retention | `Retention` | `retention` | `retention` |

Append and checkpoint operations require an explicit idempotency key. A
checkpoint also requires the caller's nonzero member generation. `reset` is the
only operation allowed to rewind. The first accepted generation is 1; another
member must take exactly the next generation.

Fetch limits are 1–1,000 records. Offsets mean the next record to fetch and are
serialized as decimal strings by the server. Go uses `uint64`, Python uses
arbitrary-precision `int`, and Java provides `BigInteger` overloads so the
complete unsigned 64-bit range remains representable.

`StreamRetentionPolicy` accepts optional per-partition record, canonical-byte,
and age limits. Go uses zero to omit a bound; Java uses `null`; Python uses
`None`. Configured values must be within 100,000 records, 3 MiB, and ten years.
Configuration and maintenance require idempotency keys. Retention observation
is linearizable and reports the active policy, watermark, base/end offsets,
retained record count, and retained canonical bytes.

## Executable examples

The complete examples append, repeat the exact append, fetch by offset, fetch
from a group checkpoint, commit that checkpoint, observe lag, configure a
combined retention policy, commit idle maintenance, and inspect retention:

- [Go regional quickstart](../console/src/quickstarts/regional/quickstart.go)
- [Java regional quickstart](../console/src/quickstarts/regional/RegionalQuickstart.java)
- [Python regional quickstart](../console/src/quickstarts/regional/quickstart.py)

The same files are embedded verbatim in the published documentation page.

Run Go:

```shell
go run ./console/src/quickstarts/regional/quickstart.go
```

Run Python:

```shell
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python
python console/src/quickstarts/regional/quickstart.py
```

Run Java:

```shell
cd sdk/java
./mvnw -q -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
export EPOCH_JAVA_CP="target/classes:$(cat target/runtime-classpath.txt)"
javac -cp "$EPOCH_JAVA_CP" ../../console/src/quickstarts/regional/RegionalQuickstart.java \
  -d target/regional-docs-classes
java -cp "target/regional-docs-classes:$EPOCH_JAVA_CP" RegionalQuickstart
```

Override `EPOCH_REGIONAL_ENDPOINTS` with a comma-separated endpoint list and
`EPOCH_TOKEN` with the scoped bearer credential.

## HTTP contract

The base route is:

```text
/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}
```

`GET` on that path performs discovery. Data operations append:

```text
/records
/groups/{group}/offsets
/groups/{group}/lag
/groups/{group}/records
/retention
/retention/maintenance
```

Every data request carries:

```text
authorization: Bearer <token>
x-epoch-resource-generation: <discovered decimal generation>
x-epoch-tablet-epoch: <discovered decimal tablet epoch>
```

Reads additionally carry
`x-epoch-read-consistency: linearizable`. Successful reads expose the exact
barrier evidence in both response headers and JSON. There is no SDK option that
silently downgrades these calls to a stale follower.

## Error and retry rules

- Authentication, scope denial, validation, idempotency conflict, and committed
  business rejection are definitive and are not rewritten as availability
  failures.
- `not_leader`, `fenced`, `route_not_found`, `route_unavailable`,
  `read_barrier_timeout`, and retryable transport/server failures allow one
  rediscovery cycle.
- A timeout can leave a mutation outcome unknown. Retry the same semantic
  request with the same idempotency key.
- After two unsuccessful discovery/operation cycles the SDK returns its typed
  unavailable/API error with the final cause. It does not loop indefinitely.

Go uses `*epoch.APIError`, Java uses `EpochApiException`, and Python uses
`EpochAPIError`.

## Verification

Focused SDK and route gates:

```shell
go test ./sdk/go/...
PYTHONPATH=sdk/python/src python3 -m unittest discover -s sdk/python/tests -p 'test_*.py'
cd sdk/java && ./mvnw verify
cargo test -p epoch-node regional_router::tests
```

The real recovery gate builds the node image, kills the active leader, runs the
Python regional SDK through append/exact-retry/fetch/checkpoint/lag and
retention configure/maintenance/observation, restarts the old voter, kills
every voter, reopens the same volumes, and verifies convergence:

```shell
make test-regional-runtime
```

## Current boundaries

This versioned alpha covers one Stream shard/partition, caller-supplied
consumer generations, and replicated time/size/combined retention. It is not
coordinated membership. Join, heartbeat,
assignment, revoke, dead-member detection, rebalance, multi-partition
ownership, automatic idle-retention scheduling, keyed compaction, legal hold,
atomic produce-and-offset transactions,
automatic batching/compression, generated response models, package-registry
publication, TLS/OIDC/mTLS, dynamic membership, and the production fault/scale
matrix remain open.
