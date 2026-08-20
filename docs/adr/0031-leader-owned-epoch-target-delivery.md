# ADR-0031: Leader-owned Epoch Queue and Stream target delivery

- **Status:** Accepted
- **Date:** 2026-08-20
- **Owners:** Rust data plane and SDKs
- **Requirements:** BUS-001, BUS-003, BUS-004, BUS-005, QUEUE-001, STREAM-001, DX-001, DX-002
- **Amends:** ADR-0020 for built-in Queue and Stream target execution

## Context

The Event Bus already replicates immutable delivery intent, transformed event
envelopes, retry policy, lease attempts, and settlement. Queue and Stream
subscription targets were previously only route metadata: an external
dispatcher could lease them, but the regional runtime did not write the target
resource.

Calling another tablet from the Bus state machine would couple two independent
Raft groups and make deterministic application depend on a remote outcome.
Calling the target before the Bus lease commits can lose ownership, while
acknowledging before the target commit can lose the event. A process or leader
can also fail after the target commits but before the Bus acknowledgement is
known. Finally, resolving a target name again after delete-and-recreate could
send one delivery to two different resource generations.

## Decision

### Ownership and durable destination binding

Every regional node runs a bounded Epoch-target worker. Only the current,
non-fail-stopped leader of the source Event Bus tablet may execute a candidate.
For each subscription the Bus exposes only its oldest due Queue or Stream
delivery.

Before acquisition, the worker resolves the target in the source Bus's exact
organization, project, environment, and namespace. Queue targets use shard
zero. Stream targets use the published `fnv1a64_utf8_mod_n_v1` contract over
the transformed envelope key, with event ID fallback.

The exact acquire command durably binds these destination coordinates:

- target kind and resource name;
- resource generation and logical shard;
- physical tablet ID and tablet epoch.

The first acquire installs the binding. Every later attempt must present the
same coordinates. A deleted, recreated, remapped, or epoch-changed target is
therefore retried or dead-lettered according to the Bus policy; the delivery
never silently migrates to a new target incarnation.

### Cross-tablet execution and unknown outcomes

The committed order is:

```text
Bus publish and route
  -> exact Bus lease plus destination binding commits
  -> destination Queue enqueue or Stream append commits
  -> Bus acknowledgement commits
```

The target command uses a bounded deterministic idempotency key derived from
the source Bus resource identity, source resource generation, source tablet ID
and epoch, delivery ID, and the pinned destination coordinates. The key does
not include the Bus attempt. If the destination commit succeeds but the source
acknowledgement is lost, a later source leader resolves the exact committed
target proposal and then acknowledges the Bus without appending a duplicate.

This is not an atomic cross-tablet transaction. A committed target write is not
rolled back if the Bus group or delivery is subsequently deleted. Progress is
at-least-once at the executor boundary and effectively-once within one pinned
Epoch target generation because that target's consensus command is
idempotent. Epoch does not claim exactly-once business processing by Queue
consumers or Stream readers.

### Ordering, backpressure, and outcomes

The Bus scheduler preserves oldest-due order per subscription and never skips
an older target record. Queue admission and ordering remain Queue-owned;
Stream partition order remains Stream-owned. Different subscriptions and
different destination shards can progress independently.

A committed target success acknowledges the Bus delivery. A recordable Queue
rejection is terminal because replaying the same destination proposal returns
the same rejection; it is dead-lettered with a bounded reason. A target that
cannot be resolved before the first lease stays unbound and pending while
topology reports the error. An unavailable, stale, or non-leading route after
binding schedules a Bus retry. Invalid target metadata or a binding mismatch
fails closed without changing the binding and is surfaced as an operator error.

### Compatibility

Legacy Event Bus commands and snapshots without signed-webhook or destination
metadata retain their v1 bytes. Signed-webhook metadata retains v2. An exact
Epoch-target acquire and a snapshot containing a destination binding require
v3. Decoders accept all three versions and reject content labeled below the
minimum version required by its fields.

## Consequences

- Replicated state machines remain deterministic and perform no cross-tablet
  I/O while applying a command.
- Target delete-and-recreate cannot redirect an ambiguous delivery.
- A destination commit followed by source failover is recoverable without a
  second Queue message or Stream record in the pinned generation.
- The worker needs both source- and destination-group availability to make
  progress, but neither group waits on the other while applying its log.
- Online Stream resharding cannot move an already-bound delivery; safe remap
  coordination remains a separate feature.
- Public pull acquisition remains a separate executor contract and does not
  weaken the built-in target binding.

## Required evidence

- v1/v2/v3 command and snapshot golden/compatibility tests;
- deterministic candidate ordering, binding, mismatch, retry, and restore
  tests;
- Queue and multi-shard Stream target tests using the shared partition vectors;
- an unknown-outcome test proving target proposal lookup avoids a duplicate;
- real three-process leader-loss, voter catch-up, and all-voter reopen evidence;
- Go, Java, and Python executable target examples; and
- operator status, semantics, API, traceability, and Pages assertions that
  state the cross-tablet non-claims.
