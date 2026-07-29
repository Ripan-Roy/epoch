# Bootstrap authentication policy v1

This directory owns the temporary cross-language authentication and
authorization contract used by the managed Go and regional Rust alpha
boundaries.

- `bootstrap-policy-v1.schema.json` is the normative JSON shape.
- `bootstrap-policy-v1.example.json` is the disposable development policy.
- `bootstrap-policy-v1-decisions.json` is the Go/Rust evaluator corpus. Its raw
  tokens are public test fixtures, not secrets.

The policy contains no raw credential. Each principal has a unique lowercase
SHA-256 token fingerprint, one or more explicit actions, and one
organization/project/environment/namespace scope. Each scope component is
either an exact identifier or the complete `*` wildcard. Partial/glob
wildcards are rejected.

Generate a non-fixture token and fingerprint without putting the token in shell
history:

```shell
read -r -s EPOCH_NEW_TOKEN
printf '%s' "$EPOCH_NEW_TOKEN" | shasum -a 256
unset EPOCH_NEW_TOKEN
```

Store only the printed fingerprint in a private policy file. Deliver the raw
credential through an appropriate secret mechanism. The current processes load
the policy only at startup:

```shell
EPOCH_AUTH_POLICY_PATH=/secure/path/bootstrap-policy.json epoch-node ...

EPOCH_AUTH_POLICY_PATH=/secure/path/bootstrap-policy.json \
EPOCH_CONTROL_REGIONAL_TOKEN='<service credential from secret storage>' \
epoch-control
```

HTTP clients use `Authorization: Bearer <token>` and RegionalAdmin gRPC clients
use the `authorization` metadata key. The complete action mapping and failure
contract are in [API contracts](../../docs/API_CONTRACTS.md#68-bootstrap-authentication-and-authorization).

This is not a production IAM system. It has no issuer/audience validation,
expiry, revocation, rotation overlap, OIDC, mTLS, replicated policy, hot reload,
or immutable audit export. See
[ADR-0011](../../docs/adr/0011-bootstrap-authz-audit-baseline.md).
