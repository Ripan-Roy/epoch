# ADR-0048: OIDC and durable authorization audit

- Status: Accepted
- Date: 2026-09-12
- Owners: security, control plane, data plane, operations
- Extends: [ADR-0011](0011-bootstrap-authz-audit-baseline.md)

## Context

The original trust boundary used static SHA-256 bearer fingerprints and emitted
authorization decisions only to process logs. That was sufficient to stop
unauthenticated alpha traffic, but it provided neither short-lived federated
identity nor durable access history. A log exporter outage could also lose the
only record of an allowed sensitive operation.

Epoch needs one contract that Go and Rust can evaluate identically, without
putting network discovery or external identity-provider availability in the
request path. It also needs to refuse allowed operations if their required
authorization record cannot reach durable local storage.

## Decision

1. Keep policy v1 compatible and add identity policy v2. V2 may contain
   fingerprinted break-glass principals, OIDC configuration, or both.
2. Verify compact JWT access tokens offline. The beta algorithm is strictly
   Ed25519/EdDSA with pinned OKP JWKs. Issuer, audience, key ID, token lifetime,
   skew, subject, token ID, roles, and four tenant-scope claims are mandatory or
   explicitly bounded. Unknown algorithms, keys, roles, duplicate JSON names,
   noncanonical base64url, and ambiguous numeric claims fail closed.
3. Build actions from the union of configured roles. The token cannot introduce
   actions directly. Build the principal ID from a truncated SHA-256 issuer
   label and the validated subject so audit records identify a stable actor
   without exposing the access token or `jti`.
4. Support emergency revocation through a bounded list of SHA-256 `jti`
   fingerprints. Multiple pinned keys provide rotation overlap. Policy and key
   changes take effect after process restart in this revision.
5. Add `audit.read` as a distinct permission. Export authorization is evaluated
   before reading, the export access is itself recorded, and returned records
   are filtered through the caller's tenant scope.
6. Write every authentication failure and authorization allow/deny to one
   owner-only canonical NDJSON journal per process. Each record links the
   previous SHA-256 digest and is fsynced before an allowed operation proceeds.
   Startup and export verify the full chain. A durable write or integrity
   failure becomes sticky and protected operations fail unavailable.
7. Store node journals on the node data PVC and the control journal on the
   control data PVC. Expose bounded decimal-string cursor pages through the
   Rust and Go HTTP boundaries. Keep structured diagnostic logging as a
   secondary sink after the durable Go write.
8. Freeze one shared golden record and JSON Schema. Both implementations must
   verify the same bytes and digest to prevent format drift.

## Consequences

OIDC callers can use short-lived, scope-bearing access tokens at the existing
HTTP and gRPC boundaries, while v1 development and break-glass tokens remain
compatible. Access decisions survive process restart and can be exported
without 64-bit browser rounding. Tampering is detectable, and audit storage
failure stops a protected allow path rather than silently losing history.

This revision deliberately does not claim complete IAM or immutable audit
delivery. It does not implement RS256/ES256, discovery/JWKS refresh, interactive
authorization-code flow, hot or replicated policy, mTLS subject-to-role
mapping, ABAC, external WORM retention, signed export bundles, policy-change
history, or the complete sensitive-operation event matrix. Those stay open in
MGD-008, MGD-011, GOV-006, and DX-005.

## Rejected alternatives

- Fetching discovery or JWKS documents synchronously was rejected because
  identity-provider latency and outages must not enter every data request.
- Accepting any JWT algorithm selected by its header was rejected because it
  expands algorithm-confusion and key-format risk without a tested contract.
- Recording only allowed requests was rejected because denied access history
  is required for investigation and abuse detection.
- Continuing after a durable audit failure was rejected because it creates
  unaudited sensitive operations.
- Calling a local hash chain externally immutable was rejected; external WORM
  delivery and retention require a separate acknowledged sink and operations
  evidence.
