# ADR-0011: Bootstrap Authentication, Scoped Authorization, and Audit Baseline

**Status:** Accepted
**Date:** 29 July 2026

## Context

The first regional alpha exposed the Go managed HTTP/gRPC APIs and Rust
regional HTTP routes without authentication. Exact-origin CORS limited browser
embedding but did not authenticate a caller. Anyone able to reach either
listener could read cross-tenant metadata or mutate desired/catalog/data state.

The target security architecture uses OIDC for humans and workloads, mTLS for
services and peers, replicated regional policy, TLS, and immutable audit
export. Those systems are not yet implemented, but leaving the new public
boundaries open until the complete target arrived would make every subsequent
M2 feature unsafe to exercise.

## Decision

Epoch introduces one deliberately bounded, versioned bootstrap policy shared by
the Go control plane and Rust regional data plane.

1. `bootstrap-policy-v1` contains a policy ID and at most 256 principals. It
   stores only unique SHA-256 bearer-token fingerprints, explicit actions, and
   exact-or-global-wildcard organization/project/environment/namespace scopes.
2. Parsing rejects unknown fields, duplicate identities/fingerprints/actions,
   unknown actions, partial wildcards, oversized documents, and malformed
   fingerprints. Authentication is deny by default and scans configured
   fingerprints with constant-time byte comparisons.
3. Public Go `/v1` HTTP and RegionalAdmin gRPC operations require bearer
   authentication. Apply/Get/Delete authorize the parsed target before state
   access or mutation; List and browser inventory filter every result by the
   principal scope.
4. `epoch-control` authenticates its catalog/route calls to Rust with a distinct
   workload principal. Rust regional catalog, route, and data paths independently
   reauthenticate and authorize the actual requested action/scope. Standalone
   `/v1` routes remain the trusted local emulator contract.
5. `EPOCH_AUTH_POLICY_PATH` is required by `epoch-control` and regional
   `epoch-node` processes. `EPOCH_CONTROL_REGIONAL_TOKEN` supplies the
   Go-to-Rust bootstrap workload credential. The console accepts a bearer token
   interactively and keeps it only in browser `sessionStorage`; credentials are
   never compiled into the static documentation/console bundle.
6. Every authentication failure and authorization allow/deny decision emits a
   bounded structured event with request ID, principal ID, policy ID, action,
   decision, reason, and tenant scope. The event type has no credential or
   payload field. Go emits JSON through `slog`; Rust emits the same stable field
   set through `tracing`.
7. Health endpoints and browser CORS preflight remain public. CORS explicitly
   permits `Authorization` and `X-Request-ID`.

## Consequences

- Managed lifecycle and regional data routes now fail closed for missing,
  malformed, invalid, action-ineligible, or cross-tenant callers.
- Go and Rust evaluate the same checked-in decision corpus, which prevents
  language-specific policy drift in this baseline.
- Development Compose and integration campaigns use named example credentials.
  Those values are public fixtures, not production secrets.
- This policy is process-local, static at startup, and based on long-lived
  bearer credentials. It does not provide OIDC, expiry/revocation, policy
  replication or hot reload, mTLS, TLS, peer authentication, encryption at
  rest, immutable audit storage/export, quota enforcement, or a production
  secrets workflow.
- The raw Go-to-Rust credential is currently injected through an environment
  variable. Production secret-file/workload-identity delivery remains required
  before this boundary can be exposed to untrusted networks.

## Rejected alternatives

- Treat exact-origin CORS or loopback publishing as authentication.
- Protect only Go and leave Rust regional endpoints directly callable.
- Put plaintext development tokens in the policy document.
- Add an unversioned allowlist independently in Go and Rust.
- Claim the bootstrap file and process logs satisfy the target IAM or immutable
  audit requirements.
