# Management SDK candidate

Status: Go implementation and local generated-wire tests; **not yet protected
delivery or complete Go/Java/Python parity**. Python and Java management clients,
real Catalog fault evidence for the public SDK, and a displayed multi-language
Pages quickstart remain open. Existing data-profile SDKs are separate.

`epoch.ManagementClient` covers the Go control service, not Rust's still-future
native data gRPC port. Use the configured controller gRPC authorities, normally
port 8081. Authorities are explicit `host:port` values, not URLs or resolver
targets. Non-loopback connections require a pinned CA and TLS 1.3; optional
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
state machine. Public Python/Java management parity remains open.

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
refresh/rotation, Python/Java management parity, package publication, and the
full SDK/version matrix remain outside this candidate's verified boundary.
