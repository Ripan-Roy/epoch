# Epoch Go SDK

This pre-alpha Go 1.26 client covers every native HTTP route currently exposed
by the standalone Epoch node. Requests use typed models and `context.Context`;
responses remain decoded JSON documents until the public wire contract freezes.

```go
package main

import (
	"context"
	"log"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
)

func main() {
	client, err := epoch.NewClient("http://127.0.0.1:7601", 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	config := epoch.DefaultStreamConfig()
	config.Durability = epoch.LocalDurable
	if _, err := client.CreateStream(context.Background(), "orders", config); err != nil {
		log.Fatal(err)
	}
	queueConfig := epoch.DefaultQueueConfig()
	queueConfig.Durability = epoch.LocalDurable
	if _, err := client.CreateQueue(context.Background(), "jobs", queueConfig); err != nil {
		log.Fatal(err)
	}
}
```

`LocalDurable` currently means fsync and recovery on one node; it does not
provide replication or protection from losing that host and its storage. Queue
messages and transitions use the same boundary; Cache and Event Bus remain
volatile in the runnable slice.

The client uses an injectable `Transport`, preserves structured and non-JSON
HTTP error bodies through `APIError`, bounds response bodies, does not follow
redirects, and performs no hidden retries for standalone operations.

`RegionalStreamClient` is the explicit replicated alternative. It accepts every
Rust node endpoint plus a `RegionalScope` and bearer token, discovers the
current leader before each call, carries generation/tablet fences, reuses the
caller's append/checkpoint idempotency key across one bounded rediscovery, and
requests linearizable fetch/lag reads. See the
[complete regional example](../../console/src/quickstarts/regional/quickstart.go)
and [contract guide](../../docs/REGIONAL_STREAM_SDK.md).

The provisional module path is not a publishable compatibility promise. Native
gRPC streaming, coordinated consumer sessions, generated response types, and
the complete Go/Java/Python contract matrix remain future work.

Run the package gate from the repository root:

```shell
go test -race ./sdk/go/epoch
go vet ./sdk/go/epoch
```
