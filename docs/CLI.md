# Epoch management CLI

The `epoch` command manages fully qualified resources through the generated
RegionalAdmin gRPC API. It keeps resource identity, enum values, 64-bit
generations, idempotency tokens, and optimistic-concurrency fences typed end to
end.

## Build and connect

```bash
go build -trimpath -o ./bin/epoch ./control/cmd/epoch

export EPOCH_CONTROL_ENDPOINT=127.0.0.1:8081
export EPOCH_CONTROL_HTTP_ENDPOINT=http://127.0.0.1:8080
export EPOCH_TOKEN=epoch-dev-admin-v1

./bin/epoch doctor
```

`doctor` requires both a successful HTTP health response and an authenticated
RegionalAdmin list probe. It never prints the bearer token.

The current alpha gRPC transport is plaintext inside the trusted local/cluster
network. TLS/mTLS and short-lived OIDC credentials are beta-hardening gates;
do not expose port 8081 to an untrusted network.

## Apply a resource

The input is strict protobuf JSON or YAML for `ApplyResourceRequest`. Unknown
fields and invalid enum names are rejected. If `request_token` is absent, the
CLI generates a cryptographically random token so the control plane can resolve
an exact retry.

```yaml
requestToken: provision-orders-v1
expectedGeneration: 0
name:
  organization: acme
  project: shop
  environment: dev
  namespace: core
  kind: RESOURCE_KIND_STREAM
  name: orders
spec:
  workloadProfile: WORKLOAD_PROFILE_STREAM_LOG
  durability: DURABILITY_PROFILE_QUORUM_DURABLE
  delivery: DELIVERY_SEMANTICS_AT_LEAST_ONCE
  ordering: ORDERING_SCOPE_PARTITION
  replicas: 3
  configuration:
    shard_count: 3
  placement:
    allowedRegions: [ap-south]
    minimumZones: 3
    requiredNodeClass: general-purpose
  governance:
    owner: team:platform
    costCenter: cc-1042
    classification: DATA_CLASSIFICATION_CONFIDENTIAL
    tags:
      service: orders
```

```bash
epoch apply --file orders.yaml
```

## Get, list, and delete

Resource names always contain all six segments:

```text
organization/project/environment/namespace/kind/name
```

```bash
epoch get acme/shop/dev/core/stream/orders

epoch list \
  --organization acme \
  --project shop \
  --environment dev \
  --namespace core \
  --kind stream \
  --page-size 100

epoch delete --expected-generation 7 \
  acme/shop/dev/core/stream/orders
```

Delete always generates a new idempotency token. Supplying
`--expected-generation` prevents an operator or automation run from deleting a
newer replacement accidentally. Output is stable protobuf JSON with proto
field names and enum names.

## Verification

```bash
go test -race ./control/cmd/epoch
go vet ./...
```

Tests cover strict YAML decoding, generated retry identities, fully qualified
names, kind mapping, list filters, delete OCC, and the two-boundary doctor
probe.
