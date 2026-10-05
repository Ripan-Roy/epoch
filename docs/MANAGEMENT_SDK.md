# Management SDK candidate

Status: Go/Java/Python implementations and local generated-wire tests;
**not yet protected delivery or complete native SDK parity**. Real Catalog
fault evidence for the public SDK and protected Pages publication remain open.
Existing data-profile SDKs are separate.

`epoch.ManagementClient` covers the Go control service, not Rust's still-future
native data gRPC port. Use the configured controller gRPC authorities, normally
port 8081. Authorities are explicit `host:port` values, not URLs or resolver
targets. Non-loopback Go connections require a pinned CA and TLS 1.3; optional
client certificates use the same existing `TLSConfig` as the HTTP SDK. The
explicit `AllowInsecureLoopback` development mode cannot select remote hosts or
coexist with TLS configuration. Proxies and resolver service policies are not
used to extend the allowlist. Bearer credentials are bounded, are never echoed
in SDK errors, and replace conflicting caller authorization metadata.

## API and result models

| Method | Contract |
|---|---|
| `ApplyResource` | Desired acceptance is not physical readiness; retain its caller token and optional OCC precondition |
| `GetResource` | Exact fully qualified identity and generation; desired and observed state remain separate |
| `ListResources` | Bounded inventory, exact supplied tenant/governance filters, no duplicate resource identities; current server has no continuation token |
| `DeleteResource` | Preserve omitted, explicit zero, and explicit nonzero generation presence |
| `BatchApplyResources` | 1–128 distinct identities, no partial success receipt, canonical result order independent of caller order |
| `GetOperation` | Original token plus the exact affected-resource set; preserve proposal ID, kind, state, cursors, and optional delete precondition |
| `WatchResourceChanges` | Bounded generated stream pages with explicit scanned-checkpoint acknowledgement and allowlisted reconnection |

The six unary calls return `(typedResponse, ManagementCallInfo, error)`.
`ManagementRPCError` retains the original status/details and unwraps its cause;
its printable error omits backend text. Call information exposes the configured
attempt budget and attempts actually started. Cancellation before dispatch is
local rejection. Once a mutation is dispatched, an unsuccessful call without a
validated receipt is conservatively `OutcomeMayBeUnknown`: even a semantic
error can follow desired-state acceptance and failed reconciliation. Do not
turn that flag into a blind retry or invent a replacement token. Resolve the
original token and full scope; `NotFound` is not an unlimited retention promise.

Within one original deadline, only `UNAVAILABLE` advances to another configured
controller. Mutation retries clone the original token, semantic payload, and
optional-field presence. Authentication, validation, OCC, throttling, fencing,
deadline expiry, and cancellation are not automatically retried. Malformed or
foreign receipts fail with `DATA_LOSS` without another mutation attempt. A
successful typed receipt does not mean the resource is ready: inspect observed
generation and conditions separately.

The current controller returns one deterministic inventory page (default 50,
maximum 100) and rejects nonempty `PageToken` values. Generated request/response
fields preserve opaque tokens for future pagination, but the present service
does not promise complete enumeration beyond that bound. Wire fixtures that
round-trip token fields test serialization, not an implemented Catalog paging
feature.

Attempts count SDK-level RPC invocations, not physical frames.
[`grpc.WithDisableRetry`](https://pkg.go.dev/google.golang.org/grpc@v1.83.2#WithDisableRetry)
disables service-policy retries; gRPC can still transparently retry a request
that was not sent or was not processed. No hedge or unbounded application retry
policy is added. Client message envelopes are bounded to 5 MiB.

## Watch consumption and checkpointing

Create a watch with an explicit cancellable context and close it when finished.
The unary timeout does not truncate the persistent watch lifetime. `Recv`
returns one validated page. Process its events and durably persist
`page.NextCursor` with the application's processing state, then call
`Acknowledge(page.NextCursor)` before receiving another page. This is an
application checkpoint, not an Epoch transaction over external side effects.

Use the **scanned** checkpoint, including for empty filtered pages: neither the
last visible event nor `LatestCursor` substitutes for `NextCursor`. The handle
keeps only one unacknowledged page. Reconnection preserves all original filters
and the last acknowledged cursor, with at most one stream per configured
endpoint over the handle's lifetime. After exhaustion, reopen explicitly from
`Checkpoint()`; it never silently resets to zero. Stale `ABORTED` cursors are
returned to the application for reconciliation, not automatically skipped.
Malformed cursors, lost retention prefixes, duplicate events, or changes outside
the requested scope fail closed without advancing the checkpoint.

Watch handles are single-consumer; `Close` may cancel from another goroutine.
Connections and independent unary calls support concurrent use. Never infer
exactly-once business processing from a management watch.

## Example and verification

### Python generated-contract foundation

The separate branch now also generates namespaced Python messages and typed
gRPC stubs from the same committed seven-method contract. Optional
`management` dependencies pin `grpcio==1.84.0` and `protobuf==7.35.1`;
`management-dev` additionally pins the compiler and typing packages. Ordinary
HTTP SDK imports retain their standard-library-only runtime.

```sh
python -m pip install 'sdk/python[management,management-dev]'
python scripts/generate-python-management.py --check
PYTHONPATH=sdk/python/src python -m unittest discover -s sdk/python/tests -v
```

Eight focused binding tests cover the seven-method source/descriptor boundary,
lazy stub construction, shared Go/Python delete-wire vectors for omitted/zero/
maximum uint64 presence, operation/watch uint64 extrema, namespaced pickling,
exact pinned regeneration, unexpected-source refusal, and executable Make
checks that reject stale Python before rewriting and preserve Go generator
failures. Strict whole-package typing checks the generated
`.pyi` contracts, with explicit constructor mapping parameters rather than
suppression. Ruff excludes external generated output, not application source.
These tests do not run a Python management RPC or implement its failover/watch
state machine on their own. The public Python client below supplies those
contracts; protected Catalog evidence remains open.

### Python management client

Import the optional API explicitly; ordinary `epoch_sdk` imports do not load
gRPC or Protobuf:

```python
from epoch_sdk.management import ManagementClient, ManagementConfig, ManagementContext
from epoch_sdk.management import ManagementRPCError, common, messages
from epoch_sdk.transport import TLSConfig

with ManagementClient(ManagementConfig(
    endpoints=("localhost:8081",), bearer_token=token, timeout=5,
    tls=TLSConfig(root_ca=ca_path, certificate=certificate_path, private_key=key_path),
)) as client:
    result = client.get_resource(messages.GetResourceRequest(name=resource_name),
                                 context=ManagementContext(timeout=3))
    print(result.response.resource.generation, result.info.attempts)
```

The six unary methods are `apply_resource`, `get_resource`, `list_resources`,
`delete_resource`, `batch_apply_resources`, and `get_operation`; each returns
`ManagementResult[generatedResponse]`. `ManagementRPCError.info` retains the
attempt count, configured budget, and conservative mutation uncertainty;
`code()` returns the status and `cause` deliberately exposes the original
gRPC error/details. Printable errors omit backend text. Predispatch validation,
cancellation, or an already-expired deadline does not claim an unknown write.

`ManagementContext(timeout=...)` supplies one monotonic caller deadline and
thread-safe `cancel()`. The SDK also caps every unary sequence with the configured
timeout. Only `UNAVAILABLE` advances once per explicit controller while preserving
the frozen token/payload/OCC. Metadata keeps caller traces/binary values and
replaces authorization with exactly one SDK credential. Remote plaintext,
resolvers/URLs, malformed metadata, duplicate endpoints, nonfinite timeouts,
unconfigured trust, and mismatched client identity files are rejected.
Provision an unencrypted PEM client key through protected deployment files;
encrypted keys are unsupported and refused without an interactive password
prompt. The shared Python HTTP trust loader uses the same non-interactive policy.

`watch_resource_changes(request, context=...)` returns a lazy single-consumer
context-managed handle. Use `page = watch.recv()`, durably process and store
`page.next_cursor`, then `watch.acknowledge(page.next_cursor)`. Its `checkpoint`
property is only the last acknowledged scanned cursor; `info` counts cumulative
stream attempts. A returned page may be mutated without changing the internal
acknowledgement value. `close()` cancels a blocked receive and the remote stream
without cancelling the shared caller context. Unary timeout does not expire
the persistent stream. `UNAVAILABLE`/EOF advances through a fixed endpoint budget;
stale `ABORTED` or malformed pages do not reset/reconnect/advance history.

TLS boundary: Python pins the explicit CA and optional client certificate/key,
but its [public gRPC credential API](https://grpc.github.io/grpc/python/grpc.html#grpc.ssl_channel_credentials)
has no client-side minimum-version selector. Unlike the Go client and Python
HTTP transport, this client does **not** independently enforce a TLS 1.3 minimum.
Use Epoch's TLS-1.3-only controller policy; do not replace it with a TLS-1.2-only
terminator and assume client-side refusal. The cross-language fixture sets both
server TLS minimum and maximum to 1.3, verifies negotiated protocol and client
identity server-side, and executes all seven Python methods. No unsupported
channel option, preflight handshake, or silent claim substitutes for this boundary.

[The complete Python example](../sdk/python/examples/management.py) is embedded
verbatim in the candidate docs page and checked by whole-package strict typing.
It covers all seven calls, a caller-owned stable token, original-scope operation
resolution, explicit OCC, bounded inventory, acknowledged watch processing, and
dedicated-resource cleanup. Its callback must process and durably checkpoint
the scanned cursor before returning. It is compile-only evidence, not a live
Catalog quickstart; do not run it against a production resource.

Twenty-one real-gRPC regression groups exercise receipt/governance validation,
bounded deadline/failover/exhaustion, original presence/tokens/metadata,
pre/post-dispatch cancellation, uint64 extrema, exact ACK/resume, filtered/empty
pages, stale/malformed history, and remote close. The separately required Python
CI step runs the Go TLS-1.3-only fixture with an explicitly configured interpreter:

```sh
EPOCH_PYTHON_MANAGEMENT_PROBE=python \
  go test -race ./sdk/go/epoch -run '^TestManagementPythonTLS13GeneratedWire$' -count=1
```

The Go-only suite may skip that interpreter-dependent probe; Python CI must not.
These fixtures prove SDK transport contracts, not durable Rust Catalog outcomes.

### Java management client

`io.epoch.sdk.ManagementClient` uses the committed generated messages under
`io.epoch.sdk.gen.epoch.v1`. Its six camel-case unary methods return
`ManagementResult<Response>` with `response()` and `info()`. Requests and
responses are immutable. `ManagementContext` holds the caller's standard gRPC
`Context` and a defensive outgoing metadata snapshot; use a cancellable context
or one with an absolute deadline. Caller traces and binary metadata are frozen,
and the SDK replaces authorization with exactly one credential.

```java
var config = new ManagementConfig(
    List.of("localhost:8081"), bearerToken, Duration.ofSeconds(5),
    new TlsConfig(caPath, clientPkcs12Path, password), false);
try (var client = new ManagementClient(config)) {
    var result = client.getResource(ManagementContext.current(),
        GetResourceRequest.newBuilder().setName(resourceName).build());
    System.out.println(Long.toUnsignedString(result.response().getResource().getGeneration()));
}
```

Java's Protobuf `uint64` values occupy the complete `long` bit pattern:
`-1L` means unsigned 18446744073709551615. Use `Long.toUnsignedString` and
unsigned comparisons, not signed nonnegative checks. `hasExpectedGeneration()`
distinguishes omission from explicit zero. The SDK preserves both across failover.
Only `UNAVAILABLE` advances once per allowlisted controller within one original
caller/configuration deadline; gRPC service-policy retry, hedging, service-config
lookup, and proxies are disabled. The JDK TLS transport permits **only TLS 1.3**,
with explicit CA and optional PKCS#12 identity loaded before channel creation.
The same existing `TlsConfig` also configures HTTPS clients.

`ManagementRPCException.info()` exposes attempts/budget/conservative unknown
mutation outcome. Printed exceptions and stack traces omit backend text.
`status()`, `trailers()`, and `rpcCause()` deliberately expose raw status/details;
`getCause()` is unset to avoid accidental chained logging. Exception serialization
is refused. A semantic mutation error can follow acceptance: resolve its original
token and exact resource set, never a freshly invented mutation identity.

`watchResourceChanges(context, request)` returns a lazy `AutoCloseable` single-
consumer handle. `receive()` validates one immutable page, and `acknowledge`
must match its exact `getNextCursor()` after durable application processing.
`checkpoint()` is only the last acknowledged scanned cursor. Empty filtered
pages also require ACK. Manual inbound flow control requests one page per receive,
with no prefetch before ACK. `close()` may unblock a receive from another thread
and cancels only its child context, not the caller's shared context. Caller
deadline/cancellation bounds the persistent stream; the unary timeout does not.
EOF/`UNAVAILABLE` advances through the fixed lifetime endpoint budget, preserving
all filters and the last acknowledged cursor. Stale/malformed pages fail closed.

[The exact Java example](../sdk/java/examples/ManagementExample.java) is embedded
verbatim in candidate Pages and compiled with `--release 25 -Xlint:all -Werror`.
It demonstrates all seven calls, a stable caller token, exact-scope operation
resolution, OCC, bounded inventory, mandatory process/checkpoint callback, and
dedicated-resource cleanup. It is compile-only, **not a live Catalog quickstart**.

Seventeen client/safety regressions pass locally alongside the full Maven
format/lint/test/package gate. Nine Java generator tests verify exact local
Protobuf 35.1/gRPC-Java 1.84.0 output, compiler checksums, safe installation,
stale/missing refusal without rewriting, unknown inventory preservation, and
Make's pre-rewrite check ordering. This avoids the remote Buf generation quota;
see [tool installation](DEVELOPMENT.md). Generated code is externally owned; only its
directory is excluded from application formatting/Checkstyle, and it still
compiles with warnings treated as errors. Java CI separately runs the Go TLS
fixture using an explicit compiled classpath:

```sh
sdk/java/mvnw --file sdk/java/pom.xml --batch-mode --no-transfer-progress \
  verify dependency:build-classpath -Dmdep.outputFile=target/runtime-classpath.txt
EPOCH_JAVA_MANAGEMENT_PROBE=java \
EPOCH_JAVA_MANAGEMENT_CLASSPATH="$(pwd)/sdk/java/target/classes:$(pwd)/sdk/java/target/test-classes:$(cat sdk/java/target/runtime-classpath.txt)" \
  go test -race ./sdk/go/epoch -run '^TestManagementJavaTLS13GeneratedWire$' -count=1
```

This executes all seven calls over real TLS 1.3/mTLS, denies wrong bearer,
anonymous client and foreign CA trust, rejects a TLS-1.2-only server before
application dispatch, and proves remote watch cancellation. Go-only SDK tests
may skip the JDK-dependent probe; Java CI must not. These are wire/identity
contracts, not Rust Catalog durability or production certification.

### Go client example

[The full Go example](../sdk/go/epoch/management_example_test.go) compiles in
the ordinary SDK test suite. The candidate docs page embeds this exact source
under SDK reference → Management SDK, with protected bundle assertions and an
explicit compile-only boundary. It covers all seven calls, explicit trust, stable
tokens, unknown-outcome resolution, presence-aware OCC, and watch checkpoints.
It is **not automatically executed against Catalog**. Use a dedicated test
resource, matching placement/governance, caller-supplied stable request tokens,
and the deployment's credential/CA/client-identity files; the example includes
cleanup of that example-owned resource.

```sh
go test -race ./sdk/go/epoch -run TestManagement -count=1
go vet ./sdk/go/epoch
```

The local fixture executes every generated method over actual TLS 1.3/mTLS
loopback sockets, denies missing client certificates and wrong bearer identity,
preserves uint64 extrema and OCC presence, and proves remote watch cancellation.
Injected-service tests separately prove exact failover, status/cause preservation,
original deadlines, post-dispatch cancellation, governance-filter enforcement,
metadata isolation, cursor acknowledgement/reconnect, endpoint exhaustion, stale
refusal, and protocol failure without unsafe mutation retry. These are client
transport/contract checks, not Rust Catalog durability or production/SLO evidence.

Native data gRPC, cooperative data consumers, background batching, identity
refresh/rotation, package publication, and the
full SDK/version matrix remain outside this candidate's verified boundary.

## Catalog fault-probe instrumentation (candidate)

Private test probes now invoke the actual public Go, Java, and Python management
clients from identical protobuf-byte plans. They support all six unaries plus
watching, persist a file-and-directory-fsynced scanned checkpoint before ACK,
and can keep a watch alive while an owned fault driver stops its controller.
The loopback-only `tests/integration/managementsdkproxy` forwards only exact,
bounded planned batches to their assigned upstream controllers, holds actual
upstream responses without returning a receipt, and exposes those byte
witnesses to the fault driver. Killing the owned proxy can then create real
socket loss; it cannot manufacture a successful commit.

Three-language relay tests pass over actual gRPC sockets: the public clients
retain their exact original token and explicit-zero OCC presence, report one
attempt and an unknown outcome after transport loss, and do not invent a new
command. Probe regressions also cover foreign commands/identity, malformed wire
data, bounded plans, uint64 checkpoint preservation, atomic replacement, and
owned transport shutdown. Cross-language CI explicitly enables and requires all
three runtimes; missing executables or classpaths fail instead of skipping.

These tests use an acknowledged fixture service, **not Rust Catalog**. A separate
owned `management_sdk_catalog.py` campaign now connects the probes to actual
Rust Catalog and Go controller fleets. It holds three real upstream batch
receipts with SDK callers pending, SIGKILLs the actual Catalog leader and then
the relay, resolves and replays the original tokens through every live
controller, compares full durable outcomes after owner/quorum/all-voter recovery,
and reconnects all three watches from application-fsynced checkpoints after
the actual controller owner's SIGKILL. Real individually committed status churn
must expire previously durable checkpoints before all three clients reject
them; it never lowers retention or substitutes cursor zero.

Replay flags are recorded, not counted as independent durable effects: a first
caller may receive a reconstructed receipt after an internal commit-result race.
The batch witness therefore requires the exact original operation and its two
contiguous generation-one creation events, alongside complete real receipts.

The independent verifier composes the complete native HA verifier with SDK
request/response, fault, per-controller, and durable-checkpoint checks. Separate
single-fleet schemas cannot claim both three/five-controller certification.
The required HA workflow runs both complete SDK/native fleets in parallel on
separate native arm64 runners, reusing this CI attempt's checksum-verified node
image. Go, Java 25, and Python 3.11 are mandatory. The protected aggregate checks
the current-attempt artifacts, identical source/image and public-probe runtime
identities, every receipt after copying, and the full independent verifier.
Failed, cancelled, skipped, missing, or incompatible fleets cannot seal a pass.
The implementation and its regression tests are a **candidate**: complete
passing live artifacts and protected CI delivery are still required. No public
SDK Catalog certification is claimed yet.

```sh
make test-management-sdk-runner
go test -race ./tests/integration/managementsdkgo ./tests/integration/managementsdkproxy
sdk/java/mvnw -f sdk/java/pom.xml -Dtest=ManagementCatalogProbeTest test
```

The ordinary Go-only test run skips the optional three-language subprocess test.
The protected cross-language workflow sets `EPOCH_MANAGEMENT_PROBE_MATRIX=1`,
supplies all four executable/classpath settings, builds the exact-source probes,
and executes `TestPublicProbeMatrixKeepsTransportLossUnknown` without cache.

With the pinned development tools/extras installed, a clean committed checkout,
and an exact-production-source node image, run the real campaign separately:

```sh
export EPOCH_REGIONAL_IMAGE=epoch/node:ha-submit-race-bf517da
export EPOCH_REGIONAL_USE_EXISTING_IMAGE=1
PYTHONPATH=sdk/python/src python3 tests/integration/management_sdk_catalog.py run \
  --output /absolute/new/sdk-catalog-evidence
PYTHONPATH=sdk/python/src python3 tests/integration/management_sdk_catalog.py verify \
  --manifest /absolute/new/sdk-catalog-evidence/evidence.json
```

The default command requires **both** controller fleets. `--controllers 3` or
`--controllers 5` runs/verifies only an explicitly isolated fleet. The campaign
builds all three public probes from the frozen checkout, records executable,
Java class/dependency, source, and image identities, uses only its own loopback
processes/Compose project, and cleans those up even on failure. It does not
change unrelated Docker services or publish packages.

To aggregate independently captured isolated fleets from the **same clean
revision and identical runtime builds**, use a new, disjoint output directory:

```sh
PYTHONPATH=sdk/python/src python3 tests/integration/management_sdk_catalog.py combine \
  --three /absolute/sdk-three/evidence.json \
  --five /absolute/sdk-five/evidence.json \
  --output /absolute/new/sdk-complete
PYTHONPATH=sdk/python/src python3 tests/integration/management_sdk_catalog.py verify \
  --manifest /absolute/new/sdk-complete/evidence.json
```

Expired Catalog history reports gRPC `ABORTED`, not `FAILED_PRECONDITION`.
The frozen `8fde109` campaign reached real three-controller leader/owner/quorum
and full-reopen recovery through all three public clients, then failed because
the new stale-plan harness expected the latter code. All three actual clients
returned `ABORTED` without a page or checkpoint write; the real floor was 397
and their saved checkpoints were 373/376/374. That failed manifest is retained,
not converted into a pass. Regression tests correct both the planned status and
the independent verifier while preserving exact saved cursors and no ACK/reset.
A fresh complete campaign and protected delivery are still required.
