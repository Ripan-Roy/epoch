# Identity, authorization, and audit contracts

This directory owns the bounded cross-language trust contracts used by the Go
control plane and Rust regional data plane.

## Policy formats

- `bootstrap-policy-v1.schema.json` and its example retain the development
  SHA-256 bearer-fingerprint format.
- `identity-policy-v2.schema.json` and its example add offline-verifiable OIDC
  access tokens while preserving optional break-glass bootstrap principals.
- `bootstrap-policy-v1-decisions.json` is the shared Go/Rust v1 authorization
  corpus. Its raw tokens are public test fixtures, not secrets.

Both versions are deny by default, reject unknown or duplicate JSON fields,
bound every collection and string, and grant only explicit actions within an
organization/project/environment/namespace scope. A scope component is either
an exact identifier or the complete `*` wildcard; partial wildcards are
rejected.

Policy v2 accepts compact JWT access tokens signed with Ed25519 (`alg=EdDSA`)
and an explicitly configured `kid`. Each issuer entry pins:

- an exact HTTPS `iss` URL (including any provider path or trailing slash) and
  1–16 accepted audiences;
- 1–16 public OKP/Ed25519 JWKs for rotation overlap;
- a maximum token lifetime of at most 24 hours and at most five minutes of
  clock skew; and
- a role-to-action map plus claim names for all four scope components.

A token must have an exact configured issuer and audience, a valid signature,
canonical integer `iat`, `nbf` (optional), and `exp`, a stable bounded `sub`, a
unique bounded `jti`, 1–16 known roles, and all scope claims. The policy may
hold up to 4,096 lowercase SHA-256 `jti` fingerprints for emergency revocation.
Authentication exposes a stable anonymized issuer/subject principal ID; raw
tokens and token IDs have no audit field.

The verifier intentionally performs no network discovery. Administrators copy
trusted Ed25519 public keys from their issuer into the policy, overlap old and
new keys during rotation, and restart the processes after changing the mounted
policy. RS256/ES256, JWKS refresh, token exchange, hot policy reload, and a
replicated policy authority remain explicit non-claims of this beta contract.

Generate a non-fixture bootstrap fingerprint without putting the token in shell
history:

```shell
read -r -s EPOCH_NEW_TOKEN
printf '%s' "$EPOCH_NEW_TOKEN" | shasum -a 256
unset EPOCH_NEW_TOKEN
```

Store only the fingerprint in a private policy file. Deliver bootstrap tokens
through a secret mechanism. Start both processes against the same policy:

```shell
EPOCH_AUTH_POLICY_PATH=/secure/path/identity-policy.json \
EPOCH_AUDIT_PATH=/var/lib/epoch/audit.ndjson \
epoch-node ...

EPOCH_AUTH_POLICY_PATH=/secure/path/identity-policy.json \
EPOCH_CONTROL_AUDIT_PATH=/var/lib/epoch-control/audit.ndjson \
EPOCH_CONTROL_REGIONAL_TOKEN='<service credential from secret storage>' \
epoch-control
```

HTTP clients use `Authorization: Bearer <token>`; RegionalAdmin gRPC clients use
the `authorization` metadata key. The full route/action mapping is in
[API contracts](../../docs/API_CONTRACTS.md#68-identity-authentication-and-authorization).

## Durable audit journal v1

`audit-journal-v1.schema.json` defines one canonical NDJSON record and
`audit-journal-v1.example.ndjson` is a Go/Rust golden integrity fixture. Each
record has a decimal string sequence, the previous SHA-256 link, a bounded
credential-free authorization event, and its own SHA-256 digest. The digest is:

```text
SHA-256(
  "epoch/audit-journal/v1\0" ||
  uint64_be(sequence) ||
  previous_digest ||
  uint64_be(canonical_event_json_length) ||
  canonical_event_json
)
```

The journal is an owner-only regular file. Startup and every export verify the
complete chain; every accepted event is appended and fsynced before an allowed
operation continues. A detected write failure or live integrity failure makes
the journal sticky-failed and later protected operations return unavailable.
Canonical JSON uses the schema field order, compact UTF-8 encoding, and no HTML
escaping; the shared golden request ID contains `<`, `>`, and `&` specifically
to prevent Go/Rust encoder drift. Request IDs are limited to 128 printable
ASCII bytes; principal/policy IDs retain their contract identifier grammar;
and each non-empty scope value is `*` or an ASCII scope identifier. Empty scope
values remain valid for cluster-local or unauthenticated decision targets.
The node and control plane expose tenant-filtered pages of 1–1,000 records at
`GET /v1/admin/audit/events` and `GET /v1/audit/events`, respectively. Sequence
and timestamp values are decimal strings to prevent browser rounding.

This is tamper-evident local storage, not an external WORM archive. Retention,
remote immutable delivery and acknowledgement, policy-change events, complete
sensitive-operation taxonomy, and cross-node reconciliation remain required
before MGD-011 can be marked complete. See
[ADR-0048](../../docs/adr/0048-oidc-and-durable-authorization-audit.md).
