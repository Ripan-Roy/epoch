# Epoch

## Product Requirements Document

**Tagline:** One runtime. Every real-time workload.  
**Document version:** 0.3  
**Date:** 22 July 2026  
**Status:** Product concept with approved brand and implementation direction  
**Audience:** Founders, product, distributed-systems engineering, infrastructure, security, and design

---

## 0. Executive decision

Epoch should not be built as one universal execution path that pretends a cache, a durable log, a work queue, and an event router are the same thing. They are not.

Epoch should be one platform with a shared control plane, storage primitives, security model, observability layer, and billing surface, exposed through four explicit workload profiles:

1. **Cache and State** — Redis-like in-memory speed, rich data structures, TTLs, eviction, atomic operations, and optional quorum durability.
2. **Stream Log** — Kafka-like partitioned durability, replay, retention, compaction, consumer offsets, transactions, and tiered storage.
3. **Work Queue** — RabbitMQ-, SQS-, Service Bus-, and Cloud Tasks-like acknowledgements, leases, retries, scheduling, priorities, dead-lettering, and competing consumers.
4. **Event Bus** — EventBridge-, Event Grid-, SNS-, and Pub/Sub-like filtering, fan-out, push and pull delivery, schemas, transformations, connectors, and archive/replay.

This is the central product choice. It preserves the qualities users actually want:

- Redis-class speed where memory and locality matter.
- Kafka-class durability and replay where an ordered log matters.
- RabbitMQ-class routing and delivery control where work distribution matters.
- Cloud-service simplicity where teams want autoscaling, integrations, security, and low operations burden.

The profiles share infrastructure where doing so improves simplicity and economics. They remain separate where forcing one behavior would make the product slower, less reliable, or semantically misleading.

### Approved working name

**Epoch** (pronounced “EP-ock”) is the approved working product name. It is one short, professional word with a strong technical meaning: an epoch is both a reference point in time and a distinct era. That fits an infrastructure platform built around durable event history, ordered streams, current state, and a new operating model for real-time systems.

The product vocabulary is clean: Epoch Cache, Epoch Streams, Epoch Queues, Epoch Bus, Epoch Cloud, and the `epoch` CLI.

The primary tagline is:

> One runtime. Every real-time workload.

Epoch remains a working brand until formal trademark, company-name, domain, social-handle, GitHub, crates.io, npm, package-manager, and app-store clearance is complete. A preliminary web screen found unrelated organizations using Epoch, including [Epoch AI](https://epoch.ai/) and an [employee-engagement product named Epoch](https://www.epochapp.com/); it did not establish legal availability.

### Product promise

> Choose the behavior you need, not a separate infrastructure stack.

### One-sentence pitch

Epoch is a cloud-neutral, multi-protocol real-time data platform that lets applications cache state, publish replayable streams, dispatch reliable work, and route events through one operational and security model.

### Approved implementation decision

Epoch will use a deliberately small polyglot stack:

- **Rust is the primary product language.** Every component that stores, replicates, routes, transforms, or delivers customer data runs in Rust.
- **Go is the managed control-plane language.** Fleet management, placement orchestration above the regional node layer, autoscaling, hosted-service APIs, billing/metering services, and the Kubernetes operator run in Go.
- **TypeScript and React power the web console.**
- **Client SDKs are native to their ecosystems:** Go, Java, and Python first, followed by TypeScript, .NET, and Rust.

Rust and Go communicate through versioned Protobuf/gRPC contracts. Go code must not directly read or mutate Epoch storage files, replication logs, queue acknowledgement indexes, transaction state, or cache memory. The Rust data node remains independently operable when the hosted Go management plane is temporarily unavailable.

Elixir and C++ are not part of the initial implementation stack. Adding a new server-side language requires an architecture decision record showing a capability that Rust or Go cannot reasonably provide.

---

## 1. The problem

Modern systems commonly operate several overlapping infrastructure products:

- Redis or a managed equivalent for cache, ephemeral coordination, counters, rate limits, sessions, and fast state.
- Kafka, Kinesis, Event Hubs, or Pub/Sub for event streams, replay, analytics ingestion, and change-data capture.
- RabbitMQ, SQS, Service Bus, Queue Storage, or Cloud Tasks for background work, request buffering, retries, and scheduled execution.
- EventBridge, Event Grid, SNS, Eventarc, or Pub/Sub for integration events, fan-out, filters, webhooks, and service-to-service routing.

The result is duplicated work:

- Separate clusters, SDKs, authentication models, network policies, monitoring, quotas, and incident procedures.
- Multiple ways to express retention, retry, ordering, encryption, tenancy, and disaster recovery.
- Data copied between systems to obtain a behavior missing in the original system.
- Application coupling to cloud-specific APIs and operational assumptions.
- High migration costs when requirements move from “fast but ephemeral” to “durable and replayable,” or from “stream” to “task queue.”
- Engineers regularly misapply delivery guarantees, especially “exactly once,” and discover the limitation only during failure.

The user does not fundamentally want four products. The user wants four behavior families with explicit trade-offs, one management experience, and reliable migration paths between them.

---

## 2. Research conclusions

### 2.1 What the reference products actually optimize

| Product family | Primary optimization | Essential mechanics | Important limitation Epoch must make explicit |
|---|---|---|---|
| Redis | Extremely low-latency access to memory-resident data structures | Single-shard command serialization, TTL and eviction, pipelining, scripting/transactions, optional RDB and AOF persistence | Redis Pub/Sub is at-most-once; asynchronous replication and persistence modes expose acknowledged-loss trade-offs |
| Kafka | Durable, ordered, replayable partition logs at high throughput | Append-only segments, batching, sequential I/O, partition leaders and replicas, offsets, retention/compaction, idempotence and transactions | Ordering is per partition, not global; end-to-end exactly-once outside Kafka needs sink cooperation |
| RabbitMQ | Flexible routing and controllable work delivery | Exchanges and bindings, durable queues, acknowledgements, publisher confirms, redelivery, priorities, TTL, dead-lettering | Queue and stream semantics differ; replication and durability depend on queue type and confirmation policy |
| Managed cloud services | Operational simplicity and ecosystem integration | Autoscaling, multi-zone placement, IAM, private networking, monitoring, connectors, serverless billing, archive/replay | Each service covers a subset, and guarantees, limits, and APIs differ by cloud and tier |

Redis offers snapshots, an append-only file, both, or no persistence; the fsync policy deliberately trades latency for possible data loss. Its Pub/Sub delivery is explicitly at-most-once, while Redis Streams adds durable entries, consumer groups, acknowledgements, and pending-entry tracking. Sources: [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/), [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/), and [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/).

Kafka stores events independently of whether they have been consumed, orders them within a partition, and uses batching, sequential I/O, page cache, compression, and replication to combine throughput and durability. Kafka documents at-most-once, at-least-once, and exactly-once modes, but its strongest processing guarantee is scoped to coordinated Kafka reads and writes; external systems must participate or be idempotent. Sources: [Kafka introduction](https://kafka.apache.org/intro/), [Kafka design](https://kafka.apache.org/42/design/design/), and [Kafka tiered storage](https://kafka.apache.org/42/operations/tiered-storage/).

RabbitMQ separates routing from queue storage. Direct, fan-out, topic, and header exchanges implement different routing rules; quorum queues use Raft for replicated durable work queues; streams provide replicated append-only logs and repeatable reads. Consumer acknowledgements and publisher confirms cover different sides of delivery. Sources: [AMQP concepts](https://www.rabbitmq.com/tutorials/amqp-concepts), [quorum queues](https://www.rabbitmq.com/docs/quorum-queues), [streams](https://www.rabbitmq.com/docs/streams), and [confirms](https://www.rabbitmq.com/docs/confirms).

### 2.2 Managed-service feature map

This table identifies the feature classes Epoch must cover. It does not claim that every named service has identical semantics.

| Need | AWS reference services | Azure reference services | GCP reference services | Epoch implication |
|---|---|---|---|---|
| Managed cache / fast state | ElastiCache, MemoryDB | Azure Managed Redis | Memorystore for Valkey, Redis, Redis Cluster, Memcached | Serverless and dedicated modes; memory-first latency; durability option; backups; sharding; replicas; cross-region DR |
| Managed Kafka | Amazon MSK and MSK Serverless | Event Hubs Kafka endpoint | Managed Service for Apache Kafka | Kafka wire compatibility, managed partition placement, connectors, autoscaling, tiered storage |
| Native event stream | Kinesis Data Streams | Event Hubs | Pub/Sub | Native API with elastic partitions, consumer modes, retention, replay, capture/export |
| Durable work queue | SQS, Amazon MQ | Service Bus, Queue Storage | Cloud Tasks | Lease/visibility model, ack/nack, retries, schedule/delay, dedupe, FIFO/session, DLQ, rate limits |
| Event routing / fan-out | EventBridge, SNS, EventBridge Pipes | Event Grid | Eventarc, Pub/Sub | Rule engine, topics/subscriptions, filters, push/pull/webhooks, transformations, enrichment, archive/replay |
| Schema governance | EventBridge Schemas | Event Hubs Schema Registry | Pub/Sub schemas | Versioned Avro, JSON Schema, and Protobuf registry with compatibility enforcement |
| Managed integration | MSK Connect, EventBridge Pipes, API destinations | Event Grid handlers, Functions integrations, Event Hubs Capture | Kafka Connect, Eventarc pipelines, export subscriptions | Connector runtime, source-to-target pipes, transforms, secrets, retries, health and lag |

Relevant official service overviews include:

- AWS: [MSK](https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html), [SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html), [Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/introduction.html), [EventBridge Pipes](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-pipes.html), [SNS](https://docs.aws.amazon.com/sns/latest/dg/welcome.html), [MemoryDB](https://docs.aws.amazon.com/memorydb/latest/devguide/what-is-memorydb.html), and [ElastiCache](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/WhatIs.html).
- Azure: [Azure Managed Redis](https://learn.microsoft.com/en-us/azure/redis/overview), [Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-about), [Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview), [Queue Storage](https://learn.microsoft.com/en-us/azure/storage/queues/storage-queues-introduction), and [Event Grid](https://learn.microsoft.com/en-us/azure/event-grid/overview).
- GCP: [Memorystore](https://cloud.google.com/memorystore), [Managed Service for Apache Kafka](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/overview), [Pub/Sub](https://docs.cloud.google.com/pubsub/docs/overview), [Cloud Tasks](https://docs.cloud.google.com/tasks/docs/dual-overview), and [Eventarc](https://docs.cloud.google.com/eventarc/docs).

Specific managed-service lessons absorbed into the requirements:

| Observed capability or constraint | Product lesson for Epoch |
|---|---|
| SQS uses visibility timeouts, retries, DLQs, delay, and separate Standard/FIFO behavior | Queue guarantees and ordering must be deliberate resource choices, with retry/redrive state visible |
| EventBridge combines buses, filtering, schemas, archive/replay, and Pipes with transform/enrichment | Event routing needs governance and integration primitives, not only broker fan-out |
| Kinesis enhanced fan-out gives registered consumers dedicated read throughput | Stream consumers need shared and isolated bandwidth choices |
| MemoryDB uses a multi-zone transactional log, while cache services also expose faster persistence trade-offs | Durable state can remain memory-first, but acknowledgement policy must be explicit |
| Event Hubs combines partitions, consumer groups, Kafka/AMQP access, Capture, schemas, and geo features | Protocol compatibility, archival capture, and managed placement belong in one stream experience |
| Service Bus includes sessions, transactions, filters/actions, scheduling, deferral, dedupe, DLQs, and auto-forwarding | Enterprise queues require more than send/receive/ack |
| Event Grid supports CloudEvents, HTTP and MQTT, push and pull delivery, filtering, and shared subscriptions | The event bus must serve both service integration and device/pub-sub patterns |
| Pub/Sub supports ordering keys, replay/seek, filters, schemas, push/pull/export, DLQs, and a carefully scoped pull exactly-once mode | Ordering and exactly-once claims must name their key, region, subscription, and acknowledgement scope |
| Cloud Tasks adds future scheduling, dispatch rate/concurrency controls, retries, HTTP targets, and named-task deduplication | A work queue must protect downstream services, not just store backlog |
| Memorystore and managed Redis offerings distinguish replicas, persistence, backups, flash tiers, and cross-region replication | “Managed cache” is a family of latency, durability, capacity, and DR profiles |

### 2.3 The non-negotiable design conclusion

“Fast, durable, ordered, globally consistent, infinitely replayable, feature-rich, and cheapest” is not one point in the design space.

Epoch must expose trade-offs as named service levels rather than hide them:

- A memory-only cache write can be microsecond-class but may be lost during process or node failure.
- A regionally quorum-committed write can have zero acknowledged loss for a defined fault model, but requires network and storage coordination.
- An asynchronously replicated cross-region write can remain fast but has a non-zero regional-disaster recovery point.
- A globally synchronous write can reduce data-loss exposure but cannot preserve local-cache latency across continents.
- Global ordering limits parallelism. Partition-key ordering permits horizontal scale.
- Exactly-once effects cannot be guaranteed for an arbitrary external database or API unless that system participates in a transaction or handles idempotency.

This honesty is a product feature. Each SDK, console flow, and API response must state the selected guarantee and its cost.

---

## 3. Product vision

### 3.1 Vision

Make real-time infrastructure composable: one resource model, one identity and policy layer, and one operating experience across low-latency state, replayable streams, reliable tasks, and routed events.

### 3.2 Product principles

1. **Semantics before compatibility.** Never claim a guarantee the system cannot prove.
2. **Profiles, not a lowest common denominator.** Preserve the distinctive strengths of cache, log, queue, and bus workloads.
3. **One envelope, several views.** Store a payload once where practical and expose stream, queue, and route state as separate indexes or views.
4. **Fast path stays short.** Cache operations must not traverse the durable-log or routing path unless the user selects durability or change capture.
5. **Durability is acknowledged, not implied.** A write is durable only after the configured commit policy succeeds.
6. **Compatibility earns migrations.** Existing clients should connect with minimal code changes, but every compatibility claim must be versioned and tested.
7. **Managed by default, portable by design.** Offer a first-class hosted service and a Kubernetes deployment without splitting the data model.
8. **Failure is part of the API.** Redelivery, fencing, lag, failover, replay, and regional recovery are visible and testable behaviors.
9. **Cost follows chosen guarantees.** Users should not pay quorum-write and long-retention costs for disposable cache entries.
10. **No silent semantic conversion.** Moving a record from a stream to a queue creates explicit delivery state; it does not silently change what an offset means.

---

## 4. Goals, non-goals, and scope

### 4.1 Goals

- G1. Provide first-class cache, stream, queue, and event-bus resource types in one platform.
- G2. Match the defining behavior of Redis, Kafka, and RabbitMQ rather than merely imitate their command names.
- G3. Cover the major managed-service feature classes present across AWS, Azure, and GCP.
- G4. Support native APIs plus practical RESP3, Kafka, AMQP, MQTT, HTTP, and CloudEvents compatibility.
- G5. Run as a managed multi-tenant service, dedicated managed cluster, or self-hosted Kubernetes deployment.
- G6. Provide explicit durability, ordering, consistency, and delivery profiles.
- G7. Allow streams to feed queues, buses, tables, and connectors without application-operated bridge services.
- G8. Make migrations observable, reversible, and testable.
- G9. Deliver a credible path to Redis-like latency and Kafka-like durable throughput on equivalent hardware.
- G10. Reduce the operational surfaces a platform team must own.

### 4.2 Non-goals

- NG1. A relational database, arbitrary SQL engine, data warehouse, or general object store.
- NG2. Transparent global ACID transactions over every resource and region.
- NG3. Magical exactly-once side effects against arbitrary external systems.
- NG4. One physical storage format or execution path for every workload.
- NG5. Complete command-level and edge-case parity with every Redis, Kafka, RabbitMQ, and cloud-service version at initial GA.
- NG6. A full stream-processing framework comparable to Flink or Beam in v1. Stateful joins and windows may arrive later.
- NG7. A workflow orchestrator with durable function state, human steps, and complex DAG semantics in v1.
- NG8. Reimplementing every proprietary cloud integration. Epoch will provide a connector framework and prioritize high-value connectors.
- NG9. Active-active conflict-free behavior for arbitrary mutable data structures in v1.

### 4.3 What “all features are covered” means

The north-star catalog covers every major behavior class found in the researched products. It does not mean every proprietary limit, UI toggle, obscure protocol extension, or historical command is present in the first release.

Every catalog item receives one of four states:

- **P0:** required for a production-worthy core.
- **P1:** required for public beta or initial GA.
- **P2:** north-star expansion after initial GA.
- **Explicitly deferred:** out of scope until a named technical dependency or customer threshold is met.

The public compatibility matrix must distinguish supported, partially supported, translated, and unsupported behavior.

---

## 5. Target users and jobs

### 5.1 Personas

| Persona | Current pain | Job to be done | Epoch value |
|---|---|---|---|
| Platform engineer | Operates several clusters and cloud services with different policies | Give product teams safe self-service real-time primitives | One control plane, policy model, SLO view, Terraform provider, and runbook surface |
| Backend engineer | Must choose infrastructure before workload behavior is fully known | Build cache, event, and worker paths without learning four operational stacks | Coherent SDK, local emulator, explicit profiles, built-in bridges |
| Data / streaming engineer | Needs replay, schemas, connectors, and reliable ingestion | Move and govern high-volume events | Durable partition logs, schema registry, tiering, capture, connectors |
| SRE | Incidents cross product boundaries and guarantees are unclear | Diagnose loss, lag, redelivery, and failover | End-to-end event trace, guarantee-aware dashboards, chaos-tested behavior |
| Security / compliance engineer | IAM, keys, private networking, and audit differ by service | Apply least privilege and prove controls consistently | Unified RBAC/ABAC, mTLS/OIDC, KMS/BYOK, audit and retention policy |
| Startup / small team | Cannot staff several infrastructure specialties | Use production messaging and cache without operating clusters | Serverless projects, usage limits, sensible templates, automatic scaling |

### 5.2 Primary jobs

- “Cache a session or computed object with predictable expiry and sub-millisecond reads.”
- “Record every domain event durably and replay it months later.”
- “Distribute jobs among workers, retry failures with backoff, and quarantine poison messages.”
- “Route selected events to several services and webhooks without writing glue code.”
- “Preserve order for one customer or entity while scaling across many customers.”
- “Schedule a task for later and cap delivery rate to protect a downstream dependency.”
- “Move from Redis Streams, Kafka, RabbitMQ, or a cloud queue without rewriting the application.”
- “Run the same logical product on-premises and in a cloud, with controlled cross-environment replication.”

### 5.3 Initial beachhead

The first customer should not be “everyone using Redis, Kafka, or RabbitMQ.” The recommended beachhead is:

> Platform teams supporting 20–200 services that already run at least two of cache, stream, and queue infrastructure, and that value cloud neutrality or hybrid deployment.

They feel the consolidation pain, can evaluate infrastructure rigorously, and can adopt Epoch one workload at a time.

---

## 6. User experience and resource model

### 6.1 Resource hierarchy

- Organization
  - Project
    - Environment
      - Namespace
        - Cache or Table
        - Stream
        - Queue
        - Event Bus
        - Subscription
        - Schema
        - Pipe or Connector
        - Policy

Namespaces define default region placement, tenancy, encryption key, network access, quotas, and retention guardrails.

### 6.2 Creation flow

The console and CLI ask the user about behavior, not vendor analogies:

1. What are you building: fast state, replayable stream, reliable work, or event routing?
2. Can acknowledged data be lost?
3. What ordering scope is required: none, key, session, partition, or single resource?
4. How long must data remain replayable?
5. Is delivery pull, push, webhook, or broker protocol?
6. Is the workload bursty/serverless or provisioned/predictable?
7. Is cross-region recovery or active service required?

The UI then displays:

- Expected latency class.
- Commit and replication behavior.
- Failure coverage and stated RPO/RTO.
- Delivery and ordering semantics.
- Estimated cost drivers.
- Incompatible feature combinations.

### 6.3 Example native CLI

~~~shell
epoch stream create orders \
  --partitions auto \
  --durability quorum \
  --retention 30d \
  --compaction key

epoch queue create payments \
  --delivery at-least-once \
  --ordering session \
  --max-attempts 8 \
  --dead-letter payments-dlq

epoch cache create sessions \
  --durability volatile \
  --eviction allkeys-lru \
  --max-memory 64GiB

epoch pipe create order-worker \
  --from stream:orders \
  --filter 'type == "order.created"' \
  --to queue:fulfillment
~~~

### 6.4 Unified event envelope

Every native resource accepts a common envelope. Protocol gateways map their native records to and from it.

~~~json
{
  "id": "01J...",
  "source": "checkout",
  "type": "order.created",
  "subject": "order/1234",
  "time": "2026-07-22T10:30:00Z",
  "key": "customer-42",
  "headers": {"tenant": "acme"},
  "content_type": "application/json",
  "schema_ref": "orders/3",
  "traceparent": "00-...",
  "payload": {"order_id": "1234"},
  "deliver_at": null,
  "ttl_ms": null,
  "priority": 5,
  "dedupe_id": "checkout-1234",
  "transaction_id": null
}
~~~

Protocol-specific fields that cannot round-trip through this envelope live in a namespaced extension map. The compatibility matrix identifies any lossy translation.

---

## 7. Product architecture

### 7.1 Architectural shape

~~~mermaid
flowchart TB
    C["Clients and protocols"] --> G["Protocol gateways"]
    G --> P["Workload profiles"]
    P --> S["Shared storage primitives"]
    P --> X["Routing, schemas, connectors"]
    M["Control and managed plane"] --> G
    M --> P
    M --> S
    M --> X
~~~

The logical unity is above the execution engines. The lowest latency cache path, durable log path, and queue delivery path are optimized independently and coordinated through shared metadata and event identities.

### 7.2 Implementation language boundary

| Area | Language | Reason |
|---|---|---|
| Storage engine, WAL, snapshots, compaction, tiering | Rust | Precise memory and I/O control without a garbage collector |
| Cache, stream, queue, routing, schemas and transactional data paths | Rust | Predictable tail latency and one correctness boundary |
| Replication, consensus groups, fencing, leases and recovery | Rust | Compile-time memory/concurrency safety and shared data-plane types |
| RESP3, Kafka, AMQP, MQTT, HTTP and native protocol gateways | Rust | Parsing and network buffers remain on the same low-copy runtime as the engines |
| Local administrative API and standalone lifecycle | Rust | The package can operate without the hosted control plane |
| Hosted resource API, fleet reconciliation, autoscaling, metering and billing | Go | Fast service development and a strong cloud/Kubernetes controller ecosystem |
| Kubernetes operator and infrastructure integrations | Go | Natural fit with Kubernetes control loops and client libraries |
| Web console | TypeScript/React | Browser-native product experience |

Rust is chosen for the data plane because its ownership model provides memory safety without a garbage collector and its type system catches many concurrency errors at compile time. Tokio supplies a mature asynchronous networking runtime. Sources: [Rust ownership](https://doc.rust-lang.org/book/ch04-00-understanding-ownership.html), [Rust concurrency](https://doc.rust-lang.org/book/ch16-00-concurrency.html), and [Tokio](https://tokio.rs/tokio/tutorial).

Go is intentionally kept out of the latency-critical record path. Go's garbage collector is appropriate for management services, but it creates CPU-versus-memory work that Epoch does not need in a memory-dense cache or high-throughput broker process. Go remains a strong fit for control loops and the Kubernetes ecosystem. Sources: [Go garbage collector guide](https://go.dev/doc/gc-guide) and [Kubernetes operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

The initial repository boundary is:

~~~text
/crates       Rust data node, engines, protocols, storage and shared libraries
/control      Go hosted control-plane services
/operator     Go Kubernetes operator
/console      TypeScript/React web application
/sdk          Native client SDKs and generated protocol bindings
/spec         Protobuf, schemas, compatibility contracts and formal models
/tests        Cross-language integration, compatibility, chaos and benchmarks
~~~

Unsafe Rust is forbidden by default. It may exist only in narrowly scoped, separately reviewed low-level crates where an operating-system, storage, SIMD, or foreign-library boundary requires it. Each unsafe block requires a documented invariant, dedicated fuzz/property tests, and security review. C/C++ dependencies are minimized and isolated behind safe Rust interfaces.

### 7.3 Major components

#### A. Protocol gateway layer — Rust

- Native gRPC and HTTP APIs.
- RESP3 gateway for Redis-compatible clients.
- Kafka wire-protocol gateway for producers, consumers, groups, admin, and transactions.
- AMQP 0-9-1 and AMQP 1.0 gateways for exchanges, queues, links, acknowledgements, and settlement.
- MQTT 5 gateway for devices and event-bus use cases.
- CloudEvents 1.0 over HTTP.
- Webhook delivery and signed callbacks.

Gateways authenticate, validate quotas, normalize envelopes, preserve protocol metadata, and translate errors. They do not own durable state.

#### B. Regional metadata and data-node control — Rust

- Raft-replicated metadata groups for resource definitions, partition maps, membership, epochs, quotas, policies, and placement.
- Strongly consistent administrative changes.
- Regional controllers for partition placement, rebalancing, leader election, node-local admission, rolling data-node upgrades, and repair.
- Separate regional data-plane operation when the global management API is unavailable.
- Monotonic fencing tokens on leadership and worker leases.

#### C. Durable log engine — Rust

- Segmented append-only logs with checksums.
- Partition leader and follower replicas across failure domains.
- Configurable acknowledgement: leader, quorum, or all in-sync replicas.
- Group commit, producer batching, compression, page cache, sequential I/O, and zero-copy transfer where supported.
- Per-partition offsets and high-water marks.
- Time/size retention, keyed compaction, tombstones, and delete retention.
- Local NVMe hot tier and object-storage remote tier.
- Idempotent producer sequence tracking and bounded producer-state recovery.
- Transaction markers, read-committed isolation, atomic offset commits, and fencing.

This engine backs Stream Log resources and supplies the changelog/WAL primitive for durable state and queues.

#### D. Cache and state engine — Rust

- Memory-resident strings, hashes/maps, lists, sets, sorted sets, counters, bitmaps, probabilistic structures, geospatial values, JSON documents, and vector/search indexes in later phases.
- Shard-local single-writer or deterministic command execution to preserve atomic command semantics.
- Pipelining, multiplexed connections, batched replication, and client-side shard routing.
- TTL wheel plus active and passive expiry.
- Pluggable eviction: no-eviction, LRU/LFU approximations, volatile/all-keys, TTL-based, and random.
- Optional durable mode using a replicated changelog plus snapshots.
- Optional flash tier for cold values, with a clearly different latency class.
- Change-data stream for durable tables and selected cache events.

Volatile cache operations bypass the durable log. Durable state operations append to the local replicated changelog before acknowledgement according to the selected profile.

#### E. Queue delivery engine — Rust

- Durable payload log plus queue-specific delivery state.
- Ready, scheduled, leased/in-flight, acknowledged, expired, and dead-lettered indexes.
- Timing wheel for delay, schedule, visibility timeout, retry, and TTL.
- Consumer leases with renewal, release, reject, and acknowledgement.
- Delivery-attempt counters and poison-message policy.
- Priority bands with fairness controls.
- FIFO session groups with an exclusive renewable session lease.
- Optional content- or identifier-based deduplication windows.
- Flow control based on credits/prefetch and downstream rate limits.

Payloads need not be copied for every subscription when reference-counted views can safely address the same immutable record. Independent acknowledgement and retention state still exist per queue or subscription.

#### F. Event routing engine — Rust

- Direct, fan-out, topic-pattern, header/attribute, and content-based routing.
- Compiled filters with indexed common fields.
- Push, pull, webhook, MQTT, queue, and stream targets.
- Retry and dead-letter policy per target.
- Transformations using declarative mappings.
- Optional enrichment through a connector or sandboxed function.
- Archive and replay by time, identifier, source, type, subject, and filter.
- Global endpoint failover policy in later phases.

#### G. Schema registry — Rust data path, Go management API

- Avro, JSON Schema, and Protobuf.
- Versioning, compatibility modes, validation, tags, ownership, and deprecation.
- Schema discovery as an explicit opt-in.
- Generated client bindings where practical.
- Schema reference preserved through streams, queues, buses, captures, and connectors.

#### H. Connector and pipe runtime — Rust execution, Go lifecycle management

- Source → filter → transform/enrich → target topology.
- Kafka Connect compatibility for high-value connectors where feasible.
- Managed connectors for object storage, relational CDC, common databases, analytics warehouses, Pub/Sub/Kafka endpoints, and generic HTTP.
- Checkpointing, idempotency keys, backpressure, partial-batch retry, secrets, health, lag, and per-record error handling.
- Sandboxed WebAssembly transforms for deterministic low-latency mappings; external functions for network enrichment.

#### I. Managed and fleet plane — Go

- Serverless pools and dedicated clusters.
- Multi-zone placement, safe upgrades, repair, backup, restore, and capacity planning.
- Tenant isolation, metering, quotas, abuse controls, and noisy-neighbor protection.
- Hosted resource/API backend, web-console backend, Terraform provider, Kubernetes operator, and cloud infrastructure integrations.
- Integrated metrics, logs, traces, alerts, audit events, and service health.

---

## 8. Guarantee model

### 8.1 Durability profiles

| Profile | Acknowledgement point | Intended use | Expected fault behavior |
|---|---|---|---|
| Volatile | Applied to leader memory | Disposable cache, ephemeral presence, lossy Pub/Sub | Process or node loss may lose acknowledged writes |
| Replicated-memory | Applied to leader and one or more replica memories | Fast sessions and transient coordination | Survives selected process/node faults; simultaneous power or region loss may lose writes |
| Local-durable | Appended to leader WAL with configured group-fsync | Single-zone durable development or cost-sensitive use | Survives process restart; zone loss may lose availability or data |
| Quorum-durable | Committed by a majority across zones, with durable media policy satisfied | Streams, queues, critical state | No acknowledged loss under the documented replica and zone fault model |
| Geo-async | Regionally committed, then replicated asynchronously | Disaster recovery with low local latency | Non-zero regional-disaster RPO, exposed as replication lag |
| Geo-sync | Committed across selected regions | Rare regulated or globally critical state | Higher latency and lower partition tolerance; not a default profile |

Every acknowledgement response includes the resource epoch and commit position. Native clients may request the achieved durability metadata for audit-sensitive operations.

### 8.2 Delivery semantics

| Mode | Mechanism | Duplicate/loss behavior | Scope |
|---|---|---|---|
| At-most-once | Mark/advance before dispatch or do not retain | May lose; does not redeliver | Ephemeral notification |
| At-least-once | Retain until ack; lease and retry | No loss under stated durability model; duplicates possible | Default queue, push, and connector mode |
| Effectively-once | At-least-once plus dedupe key and idempotency window | Suppresses known duplicate identifiers within window | Queue and webhook |
| Transactional exactly-once | Idempotent producer, transaction log, read-committed consumers, atomic stream writes and offset commits | Exactly-once processing within Epoch transaction boundary | Epoch stream/table resources in one transaction domain |

Epoch must never label webhook delivery or an arbitrary external database update “exactly once.” Those targets require an idempotency key, an inbox/outbox pattern, a transactional connector, or explicit two-phase participation.

### 8.3 Ordering

- Cache commands are linearizable within one shard in quorum mode; multi-shard operations have explicit transaction constraints.
- Stream records are ordered within a partition.
- Queue FIFO is scoped to a session or message group, allowing unrelated groups to run in parallel.
- Priority affects eligible-delivery order but does not override an already leased message.
- Event bus routes preserve source order only where the source, transform, and target all support the same ordering key.
- Cross-region asynchronous replication may expose different arrival orders until conflict/reconciliation rules apply.

### 8.4 Consistency

| Operation | Default | Optional stronger/weaker behavior |
|---|---|---|
| Cache read | Leader/local linearizable for shard | Replica/stale reads for lower latency and higher read scale |
| Durable table write | Quorum linearizable within shard | Asynchronous fast write with explicit RPO |
| Stream append | Ordered per partition after commit | Leader-only acknowledgement for lower latency |
| Queue state transition | Linearizable within queue partition | None for ack state; correctness takes priority |
| Metadata mutation | Strongly consistent | No eventual mode |
| Cross-region read | Local region, possibly stale under geo-async | Route to primary or use geo-sync profile |

### 8.5 Transactions

P1 transaction scope:

- Multiple records in one stream partition.
- Multiple partitions in one regional transaction coordinator domain.
- Atomic output-stream records plus consumed-offset commit.
- Shard-local state operations plus emitted changelog/event.
- Queue enqueue plus Epoch table update when co-located in a supported transaction group.

Explicitly deferred:

- Arbitrary global transactions.
- Transactions spanning unknown external APIs.
- Unbounded cross-profile transactions that prevent partition autonomy.

Transaction coordinators use producer epochs and fencing. Timeouts and maximum touched partitions are bounded to protect recovery and availability.

---

## 9. Protocol and migration compatibility

Compatibility is a migration surface, not the internal architecture.

| Surface | Initial target | Compatibility promise | Known boundary |
|---|---|---|---|
| Native API | gRPC, HTTP, supported language SDKs | Full Epoch semantics | Preferred for new applications |
| Redis | RESP3 plus a high-value command subset | Common strings, hashes, lists, sets, sorted sets, TTL, transactions/scripts subset, Pub/Sub, Streams subset | Modules, scripts, cluster edge cases, blocking behavior, and command parity are versioned |
| Kafka | Modern producer/consumer/admin/group protocol | Produce, fetch, offsets, groups, idempotence, transactions, retention, compaction | Broker plugins and filesystem assumptions are not portable |
| RabbitMQ | AMQP 0-9-1 core and AMQP 1.0 | Exchanges, bindings, durable queues, ack/nack, confirms, prefetch, TTL, DLX | Plugin-specific features and every policy argument are not initial scope |
| MQTT | MQTT 5 | Publish/subscribe, QoS mapping, retained messages, shared subscriptions, sessions | QoS maps to Epoch delivery but does not change external side-effect guarantees |
| CloudEvents | CloudEvents 1.0 over HTTP | Lossless common envelope for supported attributes | Vendor-specific extension fields remain namespaced |
| SQS-like | Native HTTP compatibility facade after GA | Send, receive, delete, visibility, delay, FIFO groups, dedupe, DLQ | AWS IAM/signing and every service limit are not assumed |

### Migration modes

1. **Drop-in evaluation:** point a compatible client at Epoch in a non-critical environment.
2. **Mirror:** dual-write or replicate source data to Epoch; compare offsets, keys, checksums, lag, and sampled reads.
3. **Shadow consume:** Epoch consumers process without side effects and compare outcomes.
4. **Cut over consumers:** retain source writes and a rollback path.
5. **Cut over producers:** establish a final checkpoint and reverse replication window.
6. **Decommission after proof:** remove the source only after retention, rollback, and recovery gates pass.

Migration tooling must report unsupported commands/features before traffic is moved.

---

## 10. North-star feature coverage

The following catalog is the product contract for scope planning.

### 10.1 Cache and State

| ID | Requirement | Priority |
|---|---|---|
| CACHE-001 | Strings/blobs, numeric counters, hashes/maps, lists/deques, sets, sorted sets | P0 |
| CACHE-002 | Per-key and resource-default TTL; active/passive expiry; expiry events | P0 |
| CACHE-003 | No-eviction plus LRU, LFU, TTL, random, volatile/all-key policies | P0 |
| CACHE-004 | Atomic single-key and shard-local multi-key commands | P0 |
| CACHE-005 | Pipelining, multiplexing, batch APIs, connection pooling guidance | P0 |
| CACHE-006 | Compare-and-set, optimistic transaction, increment, lease/lock primitive with fencing token | P0 |
| CACHE-007 | Volatile, replicated-memory, and quorum-durable modes | P0 |
| CACHE-008 | Snapshots, WAL/changelog restore, backup, and point-in-time restore for durable resources | P1 |
| CACHE-009 | Pub/Sub channels and pattern subscriptions with explicit at-most-once semantics | P1 |
| CACHE-010 | Durable change stream / stream projection from selected state mutations | P1 |
| CACHE-011 | Bitmaps, HyperLogLog-like cardinality, Bloom/Cuckoo filters, geospatial operations | P2 |
| CACHE-012 | JSON document operations and secondary search indexes | P2 |
| CACHE-013 | Vector index and hybrid search | P2 |
| CACHE-014 | Flash/cold tier with per-resource latency disclosure | P2 |
| CACHE-015 | Selected CRDT data types for active-active geo use | Explicitly deferred |

### 10.2 Stream Log

| ID | Requirement | Priority |
|---|---|---|
| STREAM-001 | Append-only partitioned topics/streams with key-based partitioning | P0 |
| STREAM-002 | Time-, size-, and combined retention | P0 |
| STREAM-003 | Consumer groups, offsets/checkpoints, lag, reset, rewind, and replay | P0 |
| STREAM-004 | Per-partition ordering and configurable acknowledgement policy | P0 |
| STREAM-005 | Replication across zones, leader election, ISR/health visibility | P0 |
| STREAM-006 | Batching and gzip, lz4, snappy, and zstd-compatible compression paths as protocol requires | P0 |
| STREAM-007 | Idempotent producers and duplicate-sequence rejection | P1 |
| STREAM-008 | Transactions, atomic offset commit, and read-committed consumption | P1 |
| STREAM-009 | Keyed log compaction with tombstones | P1 |
| STREAM-010 | Tiered storage and transparent historical fetch from object storage | P1 |
| STREAM-011 | Auto-partitioning recommendations and safe online partition expansion | P1 |
| STREAM-012 | Push, pull, and dedicated-bandwidth consumer modes | P2 |
| STREAM-013 | Automatic capture/export to object storage in open formats | P1 |
| STREAM-014 | Cross-cluster/region replication with loop prevention and checkpoint mapping | P1 |
| STREAM-015 | Superstream/logical stream over physical partitions | P1 |

### 10.3 Work Queue

| ID | Requirement | Priority |
|---|---|---|
| QUEUE-001 | Competing consumers with ack, nack, reject, release, and redelivery | P0 |
| QUEUE-002 | Renewable visibility timeout / acquisition lease | P0 |
| QUEUE-003 | Publisher confirmation after selected durability policy | P0 |
| QUEUE-004 | Delayed and scheduled messages | P0 |
| QUEUE-005 | Retry policy with fixed/exponential backoff, jitter, max attempts, and max age | P0 |
| QUEUE-006 | Dead-letter queue with reason, original destination, attempt history, and redrive | P0 |
| QUEUE-007 | Message TTL, queue expiry, maximum length/bytes, and overflow policy | P1 |
| QUEUE-008 | FIFO within session/message group; renewable exclusive session lock | P1 |
| QUEUE-009 | Deduplication identifier and configurable dedupe window | P1 |
| QUEUE-010 | Priority bands with starvation protection | P1 |
| QUEUE-011 | Prefetch/credit flow control and per-consumer concurrency | P0 |
| QUEUE-012 | Per-queue dispatch rate, burst, concurrency, and downstream circuit breaker | P1 |
| QUEUE-013 | Deferred messages retrievable by identifier | P2 |
| QUEUE-014 | Request/reply correlation and temporary reply destinations | P2 |
| QUEUE-015 | At-least-once dead-letter forwarding for quorum queues | P1 |

### 10.4 Event Bus and Pub/Sub

| ID | Requirement | Priority |
|---|---|---|
| BUS-001 | Topics, subscriptions, direct routes, fan-out, and wildcard topic patterns | P0 |
| BUS-002 | Attribute/header and JSON-content filters | P0 |
| BUS-003 | Pull, long-poll, push, webhook, queue, and stream targets | P0 |
| BUS-004 | Retry, timeout, rate limit, and DLQ per subscription/target | P0 |
| BUS-005 | CloudEvents 1.0 envelope and HTTP delivery | P0 |
| BUS-006 | Archive and replay by time range and filter | P1 |
| BUS-007 | Declarative input transformation | P1 |
| BUS-008 | Synchronous enrichment step with strict timeout and size limits | P2 |
| BUS-009 | Schema registry integration and payload validation | P1 |
| BUS-010 | MQTT 5 sessions, retained messages, QoS mapping, and shared subscriptions | P1 |
| BUS-011 | Webhook signing, rotation, verification helpers, and replay protection | P0 |
| BUS-012 | API destinations with OAuth/API-key secret handling | P1 |
| BUS-013 | Global endpoint health routing and failover | P2 |
| BUS-014 | Event discovery/catalog with owner, schema, lineage, and sample payloads | P2 |
| BUS-015 | Function and managed-connector targets | P1 |

### 10.5 Managed platform

| ID | Requirement | Priority |
|---|---|---|
| MGD-001 | Serverless and dedicated deployment choices | P1 |
| MGD-002 | Automatic shard/partition placement and online rebalance | P0 |
| MGD-003 | Autoscale compute, memory, throughput, and storage within policy | P1 |
| MGD-004 | Multi-zone replicas and automated failover | P0 |
| MGD-005 | Cross-region async DR, promotion, planned switchover, and failback | P1 |
| MGD-006 | Backups, restore validation, point-in-time restore where semantics permit | P1 |
| MGD-007 | Rolling version and protocol upgrade with compatibility guardrails | P1 |
| MGD-008 | IAM/RBAC/ABAC, service identities, OIDC, mTLS, ACLs, and short-lived credentials | P0 |
| MGD-009 | Private endpoints, VPC/VNet peering or private link equivalents, IP policy, and egress controls | P1 |
| MGD-010 | Encryption in transit and at rest with platform-managed KMS keys and rotation | P0 |
| MGD-011 | Audit logs, immutable export, access history, and policy-change history | P0 |
| MGD-012 | Metrics, logs, traces, dashboards, alert templates, and OpenTelemetry export | P0 |
| MGD-013 | Usage metering, budgets, quotas, limits, and anomaly alerts | P1 |
| MGD-014 | CLI, core SDKs, local emulator, and Kubernetes operator | P0 |
| MGD-015 | Connector marketplace and lifecycle management | P2 |
| MGD-016 | Customer-managed encryption keys and rotation workflow | P1 |
| MGD-017 | Terraform provider | P1 |

---

## 11. Cross-cutting functional requirements

### 11.1 Control plane

| ID | Requirement | Priority |
|---|---|---|
| CTRL-001 | Declarative resource API with idempotent create/update/delete and request tokens | P0 |
| CTRL-002 | Strongly consistent metadata, monotonic resource versions, and optimistic concurrency | P0 |
| CTRL-003 | Placement constraints for region, zone, node class, dedicated tenancy, and residency | P0 |
| CTRL-004 | Safe online split, merge where supported, leader transfer, replica repair, and rebalance | P0 |
| CTRL-005 | Admission control that prevents unsafe overcommit and exposes the limiting resource | P0 |
| CTRL-006 | Change plans, dry runs, impact preview, approval policy, and rollback for risky mutations | P1 |
| CTRL-007 | Resource templates for common patterns such as cache, audit stream, worker queue, and webhook bus | P1 |
| CTRL-008 | Organization policy that constrains durability, regions, encryption, public access, and retention | P1 |

### 11.2 Schemas, transformations, and connectors

| ID | Requirement | Priority |
|---|---|---|
| INT-001 | Avro, JSON Schema, and Protobuf storage, validation, compatibility rules, and revisions | P1 |
| INT-002 | Producer-side and broker-side validation modes with observable rejection reasons | P1 |
| INT-003 | Declarative field projection, rename, constant, template, and JSON-path-like transform | P1 |
| INT-004 | Sandboxed transform runtime with CPU, memory, time, and network limits | P2 |
| INT-005 | Connector checkpoint tied to source position and target idempotency metadata | P1 |
| INT-006 | Per-record error route, batch partial failure, pause/resume, replay, and backfill | P1 |
| INT-007 | Secret references, rotation without redeploy, outbound allowlist, and least-privilege connector identity | P1 |
| INT-008 | Initial connectors: S3-compatible object storage, Azure Blob/Data Lake, GCS, PostgreSQL CDC, MySQL CDC, generic Kafka, HTTP/webhook | P1 |
| INT-009 | Subsequent connectors: common warehouses, search stores, analytics systems, and cloud-native buses | P2 |

### 11.3 Developer experience

| ID | Requirement | Priority |
|---|---|---|
| DX-001 | Official Go, Java, and Python native SDKs | P0 |
| DX-002 | Generated API documentation with guarantee, error, retry, and idempotency guidance | P0 |
| DX-003 | Local single-binary emulator with deterministic clock and fault injection | P0 |
| DX-004 | Integration-test containers and ephemeral namespaces | P0 |
| DX-005 | Console message browser with payload redaction policy, search, replay, redrive, and access audit | P1 |
| DX-006 | Explain command showing placement, limits, durability, order, delivery, retention, and cost drivers | P0 |
| DX-007 | Compatibility scanner for Redis/Kafka/RabbitMQ usage and unsupported features | P1 |
| DX-008 | End-to-end trace view from publish through route, attempt, acknowledgement, and dead letter | P1 |
| DX-009 | Official JavaScript/TypeScript, Rust, and .NET native SDKs | P1 |

### 11.4 Lifecycle and governance

| ID | Requirement | Priority |
|---|---|---|
| GOV-001 | Soft delete and recovery window for control-plane resources; explicit irreversible purge | P1 |
| GOV-002 | Legal hold / retention lock for eligible archives and streams | P2 |
| GOV-003 | Per-field or payload redaction hooks before logs and console display | P1 |
| GOV-004 | Data residency policy and region allowlist | P1 |
| GOV-005 | Resource owner, cost center, environment, classification, and custom tags | P0 |
| GOV-006 | Exportable audit trail for resource, policy, key, replay, redrive, and payload-access actions | P0 |

### 11.5 Packaging and runtime

| ID | Requirement | Priority |
|---|---|---|
| PKG-001 | One Rust data-node executable can enable Cache, Stream, Queue, and Event Bus profiles together or selectively | P0 |
| PKG-002 | The same data formats and engine code run in standalone and clustered deployments | P0 |
| PKG-003 | Standalone mode requires no hosted Go control-plane service and exposes a local administrative API | P0 |
| PKG-004 | A three-or-more-node cluster activates replicated quorum durability, automated failover, and horizontal partition placement | P0 |
| PKG-005 | Official OCI container, Kubernetes development package, and signed development binaries | P0 |
| PKG-006 | Rust applications may embed the engine as a library with explicit single-process guarantee limits | P1 |
| PKG-007 | Other language applications use a supervised child process or sidecar through loopback or a Unix-domain socket | P1 |
| PKG-008 | A parent process can start, health-check, drain, stop, and recover the local Epoch process deterministically | P1 |
| PKG-009 | Deployment mode is reported in every health/configuration response and cannot imply unavailable guarantees | P0 |
| PKG-010 | Data created in embedded or standalone mode can be imported or migrated into a cluster without application-level re-encoding | P1 |
| PKG-011 | Signed Debian/RPM and supported operating-system packages | P1 |

---

## 12. Non-functional requirements and SLOs

These are engineering acceptance targets, not claims about an unbuilt product. They must be measured on published reference hardware with payload size, client concurrency, durability mode, replication, dataset size, and failure conditions stated.

### 12.1 Availability and recovery targets

| Capability | Initial production target | Mature target |
|---|---|---|
| Regional multi-zone data plane | 99.95% monthly | 99.99% monthly |
| Regional management operations | 99.9% monthly | 99.95% monthly |
| Acknowledged loss under one node failure in quorum mode | Zero | Zero |
| Acknowledged loss under one zone failure in quorum mode | Zero when placement policy is satisfied | Zero |
| Planned leader transfer interruption | Under 5 seconds p99 | Under 2 seconds p99 |
| Unplanned leader failover | Under 30 seconds p99 | Under 10 seconds p99 |
| Geo-async replication RPO | Under 60 seconds p99 under provisioned capacity | Under 10 seconds p99 |
| Regional disaster promotion RTO | Under 15 minutes | Under 5 minutes |
| Backup restore verification | Automated at least weekly | Automated daily for protected tiers |

If quorum placement becomes unsatisfied, the system must either reject writes or visibly downgrade only when the resource policy explicitly permits it. Silent downgrade is prohibited.

### 12.2 Performance gates

#### Reference benchmark conditions

- Same-region, same-zone clients unless a cross-zone result is named.
- Small payload baseline: 1 KiB.
- At least 30 minutes steady state after warm-up.
- Dataset larger than CPU cache and, for relevant tests, larger than one node memory.
- p50, p95, p99, p99.9 and maximum reported; averages alone are insufficient.
- Sustained test at or below 70% of the identified bottleneck, plus a separate saturation curve.
- Tail behavior measured during leader loss, rebalance, snapshot, tier fetch, and rolling upgrade.

#### Targets

| Profile | Target |
|---|---|
| Volatile Cache | On equivalent hardware and supported command mix: at least 80% of Redis throughput and no more than 1.5× Redis p99 latency; design target p50 below 0.5 ms and p99 below 1.5 ms in-zone |
| Quorum-durable State | p99 write below 5 ms within a low-latency three-zone region when storage and network permit; reads retain cache-class local behavior |
| Stream Log | At least 80% of tuned Kafka throughput and no more than 1.5× p99 produce latency on equivalent replication, acknowledgement, batch, compression, and hardware settings |
| Work Queue | At least 80% of RabbitMQ quorum-queue throughput on matched semantics; p99 publish-to-ready below 10 ms and ready-to-first-delivery below 15 ms under target load |
| Event Bus | p99 broker routing overhead below 10 ms for attribute filters excluding network target time; webhook latency reported separately |
| Scheduled delivery | 99.9% made eligible within ±1 second of scheduled time under provisioned capacity |
| Tiered historical read | First-byte and sustained throughput SLOs published by hot/warm/cold tier; never blended into hot-read latency |

The comparative gates prevent marketing a slower system as “Redis-fast” or “Kafka-fast” using an easier durability mode.

### 12.3 Scale targets

Initial GA design targets:

- 10,000 logical resources per project, subject to quota.
- 100,000 partitions/shards per large dedicated deployment.
- Millions of concurrent mostly-idle client connections across a regional managed fleet.
- At least 1 MiB native record size, with a recommended small-message path and object-reference pattern for larger payloads.
- Multi-petabyte retained stream/archive capacity through object tier.
- Queue backlogs of billions of messages through partitioned queues and cold payload storage.
- Horizontal control-plane sharding so resource count does not depend on one consensus group.

These are capacity-design targets. Each release must publish verified limits lower than or equal to what has actually passed soak and recovery tests.

### 12.4 Correctness

- No successful quorum acknowledgement before the record satisfies the configured commit rule.
- No acknowledged queue deletion before the acknowledgement state is durably committed.
- At-least-once paths may duplicate but must not silently skip a committed eligible record.
- Read-committed consumers never expose aborted transactional records.
- Fenced producers, leaders, consumers, and session owners cannot mutate state after their epoch expires.
- Expiry, delay, and retry use a monotonic-time source and tolerate wall-clock adjustment.
- Recovery deterministically rebuilds indexes from logs and snapshots and verifies checksums.
- Every downgrade, data repair, truncation, or unrecoverable gap emits an immutable audit event.

### 12.5 Efficiency

- Idle resources should share fleet capacity without one process or replica set per logical resource.
- A fan-out event should avoid full payload copies when immutable shared storage and independent delivery indexes are safe.
- Compression, batching, and zero-copy are defaulted according to workload rather than exposed only as expert tuning.
- Object storage holds cold retained records; local NVMe remains a bounded hot cache.
- Autoscaling incorporates backlog/lag, memory headroom, CPU, network, disk bandwidth, partition heat, and recovery reserve.

---

## 13. Security and compliance

### 13.1 Security requirements

- TLS 1.3 where ecosystem compatibility permits, TLS 1.2 minimum where required.
- mTLS for broker/service identity and optional client identity.
- OIDC/OAuth 2.0 for human and workload access; short-lived tokens preferred.
- RBAC plus attribute and resource conditions at organization, project, namespace, resource, consumer group/subscription, and operation levels.
- Kafka/Redis/Rabbit-compatible authentication mapped to Epoch principals without weakening the native policy model.
- Encryption at rest using envelope encryption; platform-managed and customer-managed keys.
- Separate keys and rotation policy by namespace or compliance boundary.
- Private connectivity, public-access disablement, network allowlists, outbound connector controls, and DNS controls.
- Secret manager references rather than plaintext connector credentials.
- Immutable, exportable audit events for authentication, authorization denial, config change, key use, data browse, replay, redrive, export, and deletion.
- Payload browsing disabled by default for restricted classifications.
- Per-tenant quotas, memory protection, CPU/network fairness, and denial-of-service controls.
- Dependency provenance, signed releases, SBOM, vulnerability response, and reproducible build goals.

### 13.2 Threats requiring explicit design review

- Cross-tenant payload or metadata leakage through buffers, cache reuse, object keys, logs, metrics labels, or support tooling.
- Unauthorized replay or redrive causing a second business effect.
- Webhook SSRF and data exfiltration through connectors.
- Credential replay against protocol gateways.
- Schema or decompression bombs and oversized nested payloads.
- Partition-key hot spots used for resource exhaustion.
- Stale leader, producer, or session owner acting after failover.
- Object-tier tampering or rollback.
- Malicious scripts/transforms escaping the sandbox.
- Operator access to decrypted payloads.

### 13.3 Compliance roadmap

The product should architect for, but not claim before audit:

1. SOC 2 Type I, then Type II.
2. ISO 27001.
3. GDPR support with residency, retention, deletion, subprocessors, and data-processing terms.
4. HIPAA-eligible configuration after controls and BAA readiness.
5. PCI scope-reduction guidance; do not position Epoch as a primary card-data store by default.

---

## 14. Observability and operations

### 14.1 Golden signals by profile

| Profile | Required metrics |
|---|---|
| Cache | Command rate, p50–p99.9 latency, hit ratio, memory, fragmentation, eviction, expiry, hot keys, replica lag, durability lag |
| Stream | Produce/fetch rate and latency, bytes, partition heat, under-replicated/offline partitions, ISR changes, consumer lag, commit latency, transaction aborts, tier-fetch latency |
| Queue | Ready/scheduled/in-flight/dead counts, oldest age, attempts, redelivery, ack latency, lease expiry, DLQ rate, session contention, consumer capacity |
| Bus | Match rate, filter drops, route latency, target latency, retry, throttle, DLQ, archive rate, replay progress, webhook response class |
| Connector | Source lag, checkpoint, batch size, transform time, target rate, partial failures, retry age, secret/key errors |

All metrics must be scoped by tenant without unbounded-cardinality labels. Logs carry stable event/resource/request identifiers. Traces propagate W3C trace context when the protocol can carry it.

### 14.2 Operational experiences

- A single resource health page shows configuration, achieved placement, leader/replicas, durability, lag/backlog, recent changes, incidents, and recommended actions.
- A “Why is it slow?” view attributes tail latency to quota, hot partition, replication, storage, routing, target, or client.
- A “Can I lose data?” view explains the selected mode and present risk, including unsatisfied placement and geo lag.
- Consumer inspection shows owners, epochs, offsets/leases, unacked messages, last progress, and rebalance history.
- Replay/redrive previews the record count, target, possible duplicate impact, rate limit, and cost before execution.
- Automated support bundles redact payloads and secrets.

### 14.3 Operational invariants

- Capacity reserves account for one-node and one-zone failure before accepting protected workloads.
- Rebalance is throttled and abortable and cannot consume all recovery bandwidth.
- Snapshot and compaction I/O have explicit budgets.
- Rolling upgrades keep a mixed-version compatibility window and automatically stop on SLO or invariant breach.
- Repair prefers verifiable replica/object data and never invents missing records.
- Data deletion includes local segments, snapshots, remote objects, keys, and derived indexes according to documented timelines.

---

## 15. Failure behavior

| Failure | Expected behavior | User-visible evidence |
|---|---|---|
| Client times out after publish | Result may be unknown; retry with idempotency key | Request lookup by idempotency key and commit position |
| Leader process fails | Elect eligible in-sync replica; fence old leader | Epoch change, short error/retry window, no acknowledged loss in quorum mode |
| One zone fails | Continue if quorum and capacity remain; pause unsafe operations | Placement alert, degraded redundancy, capacity status |
| Quorum unavailable | Reject strong writes; stale reads only if policy permits | Explicit unavailable error, no false success |
| Consumer dies | Lease expires and message is eligible for redelivery | Attempt count and lease history |
| Poison message | Retry according to policy, then dead-letter with reason/history | DLQ event and alert |
| Downstream webhook throttles | Apply target backoff, rate limit, and retry; protect broker | Target latency/status dashboard and backlog |
| Object tier unavailable | Hot data continues; cold fetch retries or fails clearly | Tier dependency health and affected offset/time range |
| Metadata control plane unavailable | Existing data paths continue with cached signed metadata/leases for bounded interval | Admin operations unavailable; data-plane status remains explicit |
| Region fails in geo-async | Promote secondary through fenced workflow; possible loss bounded by last replicated position | RPO estimate, last safe checkpoint, promotion audit |
| Clock jumps | Monotonic timers preserve leases/delays; wall-clock schedules are re-evaluated safely | Clock anomaly event; no early duplicate acknowledgement |
| Disk corruption | Checksum failure, replica/object repair, isolate bad media | Repair event, affected segment, proof of restored checksum |

---

## 16. Packaging and deployment

### 16.1 One package, four deployment modes

All four workload profiles can ship in one package. Distributed guarantees still require distributed resources: one process cannot truthfully provide multi-machine quorum, multi-zone availability, or regional disaster recovery.

| Mode | Runtime form | Intended use | Available guarantee ceiling |
|---|---|---|---|
| Embedded | Rust library inside the application process | Unit/integration tests, desktop software, appliances and constrained edge systems | Process-local availability; volatile or local-disk durability |
| Standalone | One Rust executable, operating-system service or OCI container | Development, small installations and single-machine on-premises use | Machine-local persistence and recovery; no survival of total machine loss |
| Cluster | Three or more Rust data nodes, with optional Go operator/control services | Production self-hosted, hybrid and dedicated deployments | Quorum durability, node/zone failover, partition scale and online repair according to placement |
| Managed | Epoch-operated clustered data plane plus Go fleet/control services | Serverless and hosted dedicated customers | Multi-zone operations, backups, autoscaling, IAM, managed upgrades and optional cross-region DR |

The package contains the complete local product:

- Cache, Stream, Queue, and Event Bus engines.
- Regional metadata, replication, leases, transactions, persistence and recovery.
- RESP3, Kafka, AMQP, MQTT, HTTP, CloudEvents, and native protocol gateways as enabled.
- Local administrative, health, metrics, tracing, backup and migration APIs.
- A configuration switch for enabling only selected profiles and protocols.

The Go hosted control plane is not required for embedded or standalone operation. It orchestrates fleets of Rust nodes but is never a correctness dependency for an already running regional data path.

#### Embedded versus sidecar

The supported in-process library is initially Rust-only because it can share engine types and lifecycle rules without an unsafe cross-language ABI. Its contract explicitly excludes multi-process and multi-machine availability.

For Go, Java, Python, Node.js, .NET, and other applications, the recommended “embedded” experience is a supervised Epoch child process or sidecar:

- The application launches or discovers the packaged Epoch executable.
- Communication uses a Unix-domain socket, named pipe, or loopback connection.
- The application can health-check, drain, and stop the child deterministically.
- Data and logs live outside the application working directory by default.
- A Epoch crash does not corrupt or terminate the application runtime.
- Epoch can be upgraded independently while retaining the same on-disk format contract.

This isolation is the default because it protects both systems from crashes, memory pressure, dependency conflicts, and incompatible release cycles.

#### Distribution artifacts

- **epoch:** user-facing CLI that can launch a local node.
- **epoch-node:** Rust data-node executable.
- **epoch-embedded:** Rust crate for explicitly embedded use.
- **epoch-control:** Go hosted/dedicated fleet services.
- **epoch-operator:** Go Kubernetes operator.
- OCI images for the data node, control services, and all-in-one development mode.
- Debian/RPM packages and signed development binaries for supported operating systems.
- Helm or equivalent Kubernetes installation package.

### 16.2 Hosted offerings

1. **Serverless**
   - Shared regional fleet.
   - Automatic partitions/shards within service limits.
   - Consumption-oriented pricing.
   - Strong isolation and rate limits.
   - Best for variable workloads and low operational appetite.

2. **Dedicated**
   - Reserved data nodes or tenant-isolated pools.
   - Predictable topology, performance, maintenance window, and networking.
   - Optional customer-managed keys and advanced compliance.
   - Best for high sustained throughput and regulated workloads.

3. **Hybrid managed**
   - Data plane in the customer cloud/account; management plane operated by Epoch.
   - Requires a strict outbound-only control channel option and documented support boundary.

### 16.3 Self-hosted

- Kubernetes operator with topology-aware placement, upgrades, backup, restore, certificates, and status conditions.
- Helm or equivalent packaging.
- S3-compatible remote tier.
- OpenTelemetry and Prometheus-compatible telemetry.
- Offline/air-gapped installation path in a later enterprise release.

### 16.4 Recommended commercial/open-source boundary

Decision still required. A credible hypothesis:

- Open source the single-cluster data plane, native clients, protocol gateways, local emulator, and Kubernetes operator under a permissive or source-available license chosen after business/legal review.
- Keep the global managed control plane, serverless fleet management, enterprise governance, advanced geo orchestration, and hosted connector operations commercial.

The license must be decided before outside contributions or public code release. Compatibility with third-party client licenses and protocol test suites requires legal review.

---

## 17. Pricing hypothesis

Pricing should be behavior-based but comprehensible.

### Serverless meters

- Ingress and egress bytes.
- Request or operation units normalized for payload and compute cost.
- Hot memory GiB-hours.
- Hot disk and remote retained GiB-months.
- Queue delivery attempts and webhook/connector executions.
- Cross-region replication and network transfer.
- Reserved throughput option for predictable tail latency.

### Dedicated meters

- Data-node class and hours.
- Storage and object tier.
- Cross-region replicas.
- Managed connectors and premium support.

### Guardrails

- Price calculator shows the effect of retention, replicas, quorum, cross-region, fan-out count, and delivery retries.
- Budget caps and anomaly alerts are first-class.
- Rejected/throttled requests should not be billed like successful work.
- Internal bridge steps should be shown; avoid surprising users with hidden multiplication from fan-out.
- Offer one free development project with strict scale and retention limits.

Do not set final prices until cost benchmarks include steady state, failure reserve, repair, object requests, observability, and support overhead.

---

## 18. Product metrics

### 18.1 North-star metric

**Production workloads consolidated per active organization**, qualified by sustained traffic and SLO health.

This measures whether Epoch replaces operational surfaces rather than merely attracting experiments.

### 18.2 Adoption

- Time from project creation to first successful publish/read/ack.
- Percentage of projects reaching production traffic within 30 days.
- Number of active profile types per organization.
- Percentage of migrations completing mirror and cutover gates.
- SDK, protocol, and connector adoption.

### 18.3 Reliability and performance

- Availability and latency SLO attainment by profile.
- Acknowledged data-loss incidents: target zero for protected modes.
- Duplicate delivery rate by declared delivery mode.
- Failover, rebalance, backup restore, and geo-promotion success.
- Percentage of hot partitions automatically mitigated or clearly diagnosed.

### 18.4 Product quality

- Compatibility suite pass rate by advertised version.
- Percentage of support cases resolved using self-serve diagnostics.
- Mean time to detect and explain guarantee degradation.
- Upgrade rollback rate.
- Cost per retained/delivered GiB and per million operations at target scale.

### 18.5 Business

- Production organizations and net revenue retention.
- Consolidation expansion: organizations moving from one Epoch profile to two or more.
- Gross margin by serverless and dedicated tier.
- Churn reasons, especially performance, missing compatibility, and cost.

---

## 19. Delivery plan

This is a large distributed-systems product. A demo can be built quickly; a trustworthy multi-protocol managed platform cannot.

Assumption: a focused 12–15 person initial team, including 6–8 Rust distributed-systems/data-plane engineers, 2–3 Go control-plane/SRE engineers, 1–2 protocol/SDK engineers, 1 product/design lead, and shared security/QA support. At least two Rust engineers must have prior production storage, database, broker, or consensus experience.

### Phase 0 — Proof and specification, months 0–3

- Finalize semantics and compatibility scope.
- Build benchmark harnesses against Redis, Kafka, and RabbitMQ on controlled hardware.
- Specify logs, leases, transactions, fencing, timers, and failure invariants.
- Establish the Rust workspace, Go control-plane modules, Protobuf boundary, unsafe-code policy, and continuous cross-language tests.
- Prototype Rust Raft metadata, segmented replicated log, native envelope, and standalone package.
- Run customer discovery with at least 15 platform teams.
- Exit: quorum log survives injected failures; benchmark methodology is reproducible; three design partners commit to evaluation.

### Phase 1 — Private alpha core, months 4–8

- Native Stream and Queue resources.
- Quorum durability, consumer offsets, leases, retries, delay/schedule, DLQ, basic routing.
- Volatile Cache with strings, hashes, sets, sorted sets, TTL, eviction, and atomic shard-local operations.
- Rust standalone data node, CLI, Go/Java/Python SDKs, Go Kubernetes operator, metrics, tracing, and audit basics.
- Embedded Rust crate as an experimental interface; supervised sidecar integration for other languages.
- Single region, multi-zone dedicated topology.
- Exit: 30-day soak, fault tests, restore test, no known acknowledged-loss violation, design-partner shadow traffic.

### Phase 2 — Private beta compatibility, months 9–14

- Kafka producer/consumer/group compatibility.
- RESP3 high-value command compatibility.
- AMQP core routing, acknowledgements, confirms, prefetch, TTL, DLX.
- Durable Cache/State mode, snapshots and recovery.
- Schema registry, compaction, tiered storage, object capture.
- Console, Terraform, TypeScript/.NET SDKs.
- Migration scanner and mirror/cutover tooling.
- Exit: published compatibility matrix; comparative performance gates met for supported semantics; two customer cutovers with rollback drills.

### Phase 3 — Public beta managed service, months 15–20

- Serverless regional pools plus dedicated service.
- Event Bus, CloudEvents, webhooks, filters, archive/replay, transformations.
- MQTT 5 core.
- Initial managed connectors.
- Private networking, customer-managed keys, organization policy, billing and quotas.
- Geo-async replication and planned switchover.
- Exit: security review, SOC 2 program underway, 99.95% beta SLO demonstrated, operational on-call and support ready.

### Phase 4 — Initial GA, months 21–26

- Mature failover, capacity reserve, upgrade safety, restore verification, and DR runbooks.
- Kafka transactions/idempotence and Epoch transactional processing.
- FIFO sessions, dedupe, queue rate/concurrency controls.
- Cross-region promotion/failback.
- Compatibility certifications for named client versions.
- 99.99% eligible production tier after evidence.
- Exit: GA correctness, performance, reliability, security, documentation, support, and finance gates all signed.

### Phase 5 — North-star expansion, months 27–36+

- JSON/search/vector and probabilistic state structures.
- More connectors and event discovery.
- Global event endpoints and selected active-active CRDT state.
- Deeper cloud API compatibility facades where demand justifies them.
- Advanced streaming transforms and materialized views.

### What to cut if schedule is constrained

Cut breadth before correctness:

1. Delay MQTT, search/vector, global endpoints, and long-tail connectors.
2. Narrow Redis and AMQP command/plugin coverage.
3. Launch dedicated before serverless.
4. Keep one region before cross-region.
5. Do not cut quorum correctness, fencing, recovery tests, observability, or the honest guarantee model.

---

## 20. Key risks and mitigations

| Risk | Why it matters | Mitigation / kill criterion |
|---|---|---|
| Feature sprawl | “Everything” can prevent any profile from becoming excellent | Four profile owners, P0/P1 gates, no new profile before core reliability |
| Lowest-common-denominator product | Unification can erase the reason users chose Redis/Kafka/RabbitMQ | Independent fast paths and comparative benchmarks on matched semantics |
| Compatibility trap | Edge cases consume the roadmap and still disappoint | Versioned subsets, conformance suites, telemetry-driven prioritization, native API first |
| Incorrect exactly-once marketing | Causes financial and operational harm | Precisely scoped guarantee, idempotency tooling, external-effect documentation |
| Queue-on-log state explosion | Per-consumer ack indexes can dominate storage and recovery | Partitioned compact state, bounded attempts, snapshots, reference-counted payloads, benchmarks with huge backlog |
| Cache durability harms latency | Synchronous replication/storage contradicts volatile cache expectations | Explicit durability profiles and separate changelog path; never default it silently |
| Hot keys/partitions | One entity can cap scale | Detection, client hints, virtual shards, split guidance, rate limits; do not promise transparent reorder-free repartitioning |
| Multi-tenancy tail latency | Noisy neighbors destroy the “fast” claim | Admission control, isolation classes, per-tenant schedulers, dedicated tier, saturation testing |
| Autoscaling instability | Moving state can worsen an overload | Predictive headroom, recovery reserve, bounded movement, load shedding, rate limits |
| Split-brain or stale owners | Can corrupt ordering, ack state, and transactions | Quorum leadership, epochs, fencing tokens, lease proof, formal specification |
| Managed-service economics | Replicas, headroom, egress, and observability can erase margin | Cost model from first benchmark, remote tier, bin-packing with isolation, dedicated option |
| Weak positioning | Buyers may hear “four mediocre products” | Lead with consolidation plus explicit best-in-class profile benchmarks and migration |
| Regulatory overreach | Hosted payload infrastructure has broad compliance burden | Start with defined regions/controls; claim certifications only after audit |
| Team/time underestimation | Distributed storage and protocols are each multi-year efforts | Narrow GA, experienced hires, design partners, kill gates, 24–36 month north-star horizon |

### Product kill / pivot criteria

Reconsider the broad platform if, by the end of Phase 2:

- No design partner wants to consolidate at least two workload families.
- Cache or stream comparative performance remains below 60% on equivalent semantics after architectural optimization.
- Protocol translation dominates data-plane latency or operational failures.
- Queue delivery-state cost makes large backlogs economically uncompetitive.
- A simpler “stream + queue” product produces much stronger customer pull than the four-profile thesis.

---

## 21. Verification strategy

### 21.1 Correctness

- Formal models for leader epochs, quorum commit, queue leases, transaction fencing, and geo promotion, using TLA+ or an equivalent model checker.
- Property-based tests for logs, indexes, expiry, compaction, routing, and protocol translation.
- Linearizability testing for supported state operations.
- History checking for queue ack/redelivery invariants and transactional stream processing.
- Deterministic simulation of network partitions, clock behavior, crash/restart, message duplication/reorder, disk errors, and partial writes.
- Checksummed backup/restore and replica rebuild comparison.

### 21.2 Compatibility

- Official/open protocol test suites where licensing permits.
- Differential tests against named Redis, Kafka, and RabbitMQ versions.
- Corpus fuzzing of network frames, malformed payloads, compression, schemas, and transformations.
- Real-client matrix across supported languages and versions.
- Upgrade/downgrade tests across the published mixed-version window.

### 21.3 Performance

- Reproducible infrastructure-as-code benchmark environments.
- Workload suites for small/large records, batch sizes, key skew, fan-out, huge backlog, long retention, high connection count, transactions, and mixed profiles.
- Comparison on matched persistence, replication, acknowledgement, compression, and failure domains.
- Saturation curves and coordinated-omission-safe latency measurement.
- Cost-per-work-unit comparison, not throughput alone.

### 21.4 Resilience

- Continuous chaos in pre-production.
- Quarterly regional promotion and failback exercises before GA.
- Node/zone loss during peak load, rebalance, compaction, backup, restore, upgrade, and object-tier outage.
- Disaster recovery using only backups/object tier when all local replicas are unavailable.
- 30-, 60-, and 90-day soak tests as maturity increases.

### 21.5 Security

- Threat modeling per profile and connector.
- Static/dynamic analysis, dependency scanning, secret scanning, fuzzing, penetration tests, tenant-isolation tests, and cryptographic key rotation exercises.
- SSRF and data-exfiltration tests for webhooks/connectors.
- Authorization differential tests across native and compatible protocols.

---

## 22. GA acceptance criteria

Epoch is not GA until all of the following are true:

1. Quorum-mode failure tests demonstrate zero acknowledged loss for the documented node and zone fault model.
2. Performance gates pass on a published reference setup for every GA profile.
3. Compatibility claims name exact protocol/client versions and pass the associated test matrix.
4. Backup restore and geo promotion/failback drills meet stated RPO/RTO.
5. Every profile exposes latency, availability, durability, ordering, retention, and delivery semantics in API and console.
6. An unknown publish outcome is safely resolvable through idempotency or status lookup.
7. Tenant-isolation, authorization, encryption, audit, and connector egress reviews pass.
8. SLO monitoring, paging, on-call, escalation, incident communication, and customer support are operational.
9. Billing has been reconciled against raw usage and failure/retry cases.
10. Two or more design partners have run production workloads for at least 60 days, including one migration from an existing reference product.

---

## 23. Decisions and remaining questions

### Locked architecture decisions

- Epoch is the approved working product name; public launch remains contingent on formal clearance.
- Rust is the primary language for the data node, engines, regional metadata, replication, protocols, persistence, recovery, and local administration.
- Go is the language for hosted fleet management, autoscaling, metering/billing, cloud integration, and the Kubernetes operator.
- TypeScript/React is the web-console stack.
- Protobuf/gRPC is the versioned Rust/Go service boundary.
- The Rust data node remains independently operable without the hosted Go control plane.
- Embedded, standalone, cluster, and managed modes use the same Rust engines and on-disk format contracts.
- Elixir and C++ are not initial server-side implementation languages.

### Product

- Which two-profile combination is the initial commercial wedge: Stream + Queue is recommended.
- Is the first distribution dedicated/self-hosted, hosted dedicated, or serverless? Dedicated plus self-hosted is lower risk than serverless first.
- What degree of Redis and AMQP compatibility is necessary for the first three design partners?
- Is durable state positioned as a primary database or as durable infrastructure state? The latter is safer initially.

### Architecture

- Consensus library versus custom implementation.
- Storage abstraction and supported local filesystems/NVMe behavior.
- Partition unit shared across profiles versus profile-specific partition groups.
- Transaction coordinator scope and limits.
- Whether queue payloads and stream records share immutable storage in v1 or only through explicit pipes.
- Remote-tier object format and open export contract.

### Business and governance

- Open-source and commercial boundary and license.
- Formal Epoch trademark, company-name, domain, repository, and package-registry clearance.
- Initial cloud regions and residency promise.
- Support model and target customer size.
- Compliance sequence.
- Whether cloud-provider API facades are strategic or only migration aids.

---

## 24. Recommended next steps

### Next 30 days

1. Interview 15 platform teams that run at least two reference systems.
2. Test the problem statement, not the proposed architecture: quantify clusters, incidents, staffing, cloud lock-in, and migration pain.
3. Select three design partners and collect real workload traces or synthetic specifications.
4. Freeze the Phase 0 semantics document for log commit, leases, redelivery, transactions, ordering, and expiry.
5. Build matched Redis/Kafka/RabbitMQ benchmark environments before writing the full engine.
6. Run formal Epoch naming, trademark, package-registry, domain, and license review.

### Next 90 days

1. Demonstrate a three-node replicated log with leader fencing, checksums, recovery, and fault injection.
2. Implement one queue view over the log with leases, ack, retry, schedule, and DLQ.
3. Implement one memory shard with TTL, eviction, pipelining, snapshot, and optional changelog.
4. Package the same Rust engine in standalone and three-node modes and demonstrate format-compatible migration between them.
5. Measure whether shared primitives reduce complexity without placing the durable log in the volatile cache fast path.
6. Ship a native SDK, CLI, local emulator, and observability from the first prototype.
7. Decide whether the evidence supports the four-profile platform or a narrower Stream + Queue launch.

---

## 25. Source notes

This PRD was built from official documentation current to 22 July 2026. Product limits and preview status change; verify them again before using the document for a launch claim or compatibility contract.

### Naming screen

- [Merriam-Webster definition of “epoch”](https://www.merriam-webster.com/dictionary/epoch)
- [Epoch AI](https://epoch.ai/)
- [Epoch employee-engagement platform](https://www.epochapp.com/)

### Implementation languages and runtime

- [Rust ownership and memory safety without garbage collection](https://doc.rust-lang.org/book/ch04-00-understanding-ownership.html)
- [Rust concurrency and compile-time safety](https://doc.rust-lang.org/book/ch16-00-concurrency.html)
- [Tokio asynchronous Rust runtime](https://tokio.rs/tokio/tutorial)
- [Go garbage collector guide](https://go.dev/doc/gc-guide)
- [Kubernetes operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)

### Redis

- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/)
- [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)
- [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Redis source and data-type overview](https://github.com/redis/redis)

### Apache Kafka

- [Kafka introduction](https://kafka.apache.org/intro/)
- [Kafka 4.2 design](https://kafka.apache.org/42/design/design/)
- [Kafka tiered storage](https://kafka.apache.org/42/operations/tiered-storage/)

### RabbitMQ

- [AMQP concepts and exchange types](https://www.rabbitmq.com/tutorials/amqp-concepts)
- [RabbitMQ quorum queues](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ streams](https://www.rabbitmq.com/docs/streams)
- [Publisher confirms and consumer acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [Dead-letter exchanges](https://www.rabbitmq.com/docs/dlx)
- [TTL](https://www.rabbitmq.com/docs/ttl)
- [Priority queues](https://www.rabbitmq.com/docs/priority)
- [Negative acknowledgements](https://www.rabbitmq.com/docs/nack)
- [RabbitMQ protocols](https://github.com/rabbitmq/rabbitmq-server)

### AWS

- [Amazon MSK](https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html)
- [MSK Serverless](https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html)
- [Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Amazon SNS](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)
- [SNS message filtering](https://docs.aws.amazon.com/sns/latest/dg/sns-message-filtering.html)
- [Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/introduction.html)
- [Kinesis enhanced fan-out](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html)
- [EventBridge archives and replay](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-archive.html)
- [EventBridge schemas](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-schema.html)
- [EventBridge Pipes](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-pipes.html)
- [Amazon MQ](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/welcome.html)
- [Amazon MemoryDB](https://docs.aws.amazon.com/memorydb/latest/devguide/what-is-memorydb.html)
- [Amazon ElastiCache](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/WhatIs.html)
- [ElastiCache global datastores](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Redis-Global-Datastore.html)
- [ElastiCache data tiering](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/data-tiering.html)

### Microsoft Azure

- [Azure Managed Redis](https://learn.microsoft.com/en-us/azure/redis/overview)
- [Azure Managed Redis persistence](https://learn.microsoft.com/en-us/azure/redis/how-to-persistence)
- [Azure Managed Redis active geo-replication](https://learn.microsoft.com/en-us/azure/redis/how-to-active-geo-replication)
- [Azure Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-about)
- [Kafka on Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)
- [Event Hubs Capture](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview)
- [Event Hubs Schema Registry](https://learn.microsoft.com/en-us/azure/event-hubs/schema-registry-overview)
- [Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview)
- [Service Bus advanced features](https://learn.microsoft.com/en-us/azure/service-bus-messaging/advanced-features-overview)
- [Azure Queue Storage](https://learn.microsoft.com/en-us/azure/storage/queues/storage-queues-introduction)
- [Azure Event Grid](https://learn.microsoft.com/en-us/azure/event-grid/overview)
- [Event Grid pull delivery](https://learn.microsoft.com/en-us/azure/event-grid/pull-delivery-overview)
- [Event Grid MQTT](https://learn.microsoft.com/en-us/azure/event-grid/mqtt-overview)

### Google Cloud

- [Memorystore](https://cloud.google.com/memorystore)
- [Memorystore for Valkey](https://docs.cloud.google.com/memorystore/docs/valkey/product-overview)
- [Memorystore persistence](https://docs.cloud.google.com/memorystore/docs/valkey/persistence-overview)
- [Memorystore cross-region replication](https://docs.cloud.google.com/memorystore/docs/valkey/about-cross-region-replication)
- [Managed Service for Apache Kafka](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/overview)
- [Managed Kafka Connect](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/kafka-connect-overview)
- [Google Cloud Pub/Sub](https://docs.cloud.google.com/pubsub/docs/overview)
- [Pub/Sub subscription properties](https://docs.cloud.google.com/pubsub/docs/subscription-properties)
- [Pub/Sub exactly-once delivery](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery)
- [Pub/Sub ordering](https://docs.cloud.google.com/pubsub/docs/ordering)
- [Pub/Sub replay](https://docs.cloud.google.com/pubsub/docs/replay-overview)
- [Pub/Sub schemas](https://docs.cloud.google.com/pubsub/docs/schemas)
- [Cloud Tasks](https://docs.cloud.google.com/tasks/docs/dual-overview)
- [Cloud Tasks HTTP scheduling](https://docs.cloud.google.com/tasks/docs/creating-http-target-tasks)
- [Eventarc](https://docs.cloud.google.com/eventarc/docs)

---

## 26. Final recommendation

Build Epoch only if the team is willing to be rigorous about boundaries.

The winning product is not “Redis + Kafka + RabbitMQ in one process.” It is:

- one product and resource model,
- four workload profiles,
- a small number of proven storage and coordination primitives,
- protocol-compatible migration surfaces,
- explicit and testable guarantees,
- managed-service operations,
- and a roadmap that earns breadth after correctness.

Start with the durable log, queue delivery state, and control plane. Prove failure behavior. Add the cache fast path without forcing it through quorum storage. Then add compatibility gateways and the event-bus experience. That sequence creates a credible foundation for the broad vision without sacrificing the traits—speed, durability, routing flexibility, and operational simplicity—that made the reference systems valuable in the first place.

Implement that sequence in Rust, with Go restricted to the managed fleet/control plane. Ship the Rust engine first as one standalone package, then prove that the same binary and data formats scale into a three-or-more-node production cluster.
