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
caller's append/batch/checkpoint/session/retention idempotency key across one bounded
rediscovery, and requests linearizable fetch/lag/session/retention reads.
`StreamShardFor` implements the advertised FNV-1a UTF-8 contract, while
`AppendKeyed` selects that shard from the event key or ID and fails before
writing if the resource generation changes. The client also exposes
single-shard atomic `AppendBatch`: `EncodeStreamBatch` builds canonical none or
gzip frames, while `NewStreamBatchFrame` wraps caller-produced standard LZ4,
Snappy, or Zstd frames without changing their bytes. The same client now covers
fenced idempotent producers, same-tablet transactions/read isolation, key
compaction, checksum-verified tier fetch, manual and automatic open-format
capture, checkpointed replication ingress, partition advice, push/dedicated
long poll, and deterministic non-atomic superstreams. It also exposes
time/size/combined retention and shard-zero coordinated sessions. See the
[complete regional example](../../console/src/quickstarts/regional/quickstart.go)
and [contract guide](../../docs/REGIONAL_STREAM_SDK.md).

`RegionalQueueClient` applies that same discovery, bearer, fence, same-key
rediscovery, and linearizable-read contract to the complete replicated Queue
lifecycle: enqueue, credit-aware acquire, every lease disposition, maintenance,
DLQ/redrive history, redrive, counts, flow, mutation lookup, and status. See the
[complete Queue example](../../console/src/quickstarts/regional_queue/quickstart.go)
and [Queue contract guide](../../docs/REGIONAL_QUEUE_SDK.md).

`RegionalCacheClient` exposes strict string/blob/counter/hash/list/set/sorted-set
values, set/delete/CAS/increment, committed access for deterministic LRU/LFU,
bounded atomic batches, fenced lock lifecycle, explicit expiry maintenance,
mutation lookup, pure observation, and status over the same regional core. See the
[complete Cache example](../../console/src/quickstarts/regional_cache/quickstart.go)
and [Cache contract guide](../../docs/REGIONAL_CACHE_SDK.md).

`RegionalBusClient` exposes subscription delivery policy, publish, delivery
acquire/ack/fail/reject/maintenance, mutation lookup, archive replay, delivery query,
and status over the same regional core. Mutations keep exact caller-owned keys;
delivery settlement also preserves the opaque lease token.
`QueueTarget` and `StreamTarget` create native destinations executed by the
source Bus leader; delivery queries expose the server-pinned generation,
shard, tablet, and epoch after acquisition.
`SignedWebhookTarget` captures a configured key ID, while
`VerifyWebhookSignature` verifies the exact raw body, canonical attempt and
timestamp, constant-time HMAC-SHA-256, and returns the replay identity receivers
must persist before side effects. See the
[complete Event Bus example](../../console/src/quickstarts/regional_bus/quickstart.go)
and [Event Bus contract guide](../../docs/REGIONAL_EVENT_BUS_SDK.md).

The provisional module path is not a publishable compatibility promise. Native
gRPC streaming, background/cooperative consumer sessions, atomic
assignment-plus-offset handoff, generated response types, and package
publication remain future work.

Run the package gate from the repository root:

```shell
go test -race ./sdk/go/epoch
go vet ./sdk/go/epoch
```
