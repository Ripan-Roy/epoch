# ADR-0023: Replicated Stream time and size retention

- Status: Accepted
- Date: 2026-08-11
- Owners: Stream, tablet runtime, SDK, and recovery maintainers

## Context

Epoch previously had only a standalone record-count cap. The replicated Stream
tablet always retained every record, storage segment rotation did not delete
logical records, consumer checkpoints assumed a base offset of zero, and the
regional SDKs had no retention surface. That did not satisfy STREAM-002.

Retention is a replicated state transition. Letting each voter compare records
with its own wall clock would allow replicas to expose different base offsets,
digests, and snapshots. Using Rust allocation size would also make a byte limit
platform- and implementation-dependent.

## Decision

### One versioned mutation boundary

Canonical Stream command v4 adds a `retention` operation with two modes:

- `configure` replaces the complete policy and enforces it immediately;
- `maintain` enforces the current policy at the command's committed
  `applied_at_ms`.

Both use the existing scope-separated idempotency key and Raft proposal path.
Every voter therefore receives the same policy, time input, command order, and
typed receipt. V1 append, v2 compressed batch, and v3 consumer checkpoint bytes
remain unchanged and version/kind locked.

### Policy and hard limits

The complete Stream policy may configure any combination of:

- `max_records_per_partition`: 1 through 100,000;
- `max_bytes_per_partition`: 1 through 3 MiB;
- `max_age_ms`: 1 millisecond through 10 years.

An omitted field disables that bound. A policy with every field omitted
disables logical retention. Each enabled bound is applied independently to
every partition; the current replicated tablet exposes only partition `0`.
The tablet limits keep the retained image within the current bounded checkpoint
architecture; the profile snapshot ceiling still applies independently.

### Canonical persisted-byte accounting

The retained size of a record is the byte length of its compact canonical JSON
`StreamRecord` representation, including partition, offset, append time, and
the complete envelope. It is not Rust heap capacity, compressed request-frame
size, EPRS frame size, or filesystem allocation.

A record larger than the configured byte limit is rejected before its offset
or state changes. Batch append continues to use a cloned Stream state, so one
oversized record rejects the complete batch without partial visibility.

### Time and combined semantics

A record expires when:

`record.appended_at_ms + max_age_ms <= retention_as_of_ms`

The boundary is inclusive. The state machine computes the equivalent cutoff
with checked subtraction. Retention first removes every record at or before
the cutoff, then removes oldest records until the count and byte bounds are
also satisfied. Combined retention is therefore the union of all configured
deletion conditions.

Every append evaluates retention using the command's committed time. A
configuration mutation also evaluates immediately. An idle stream advances age
retention only when a client or operator commits `maintain`. Go, Java, and
Python expose that operation explicitly; a built-in periodic scheduler is not
part of this ADR.

The retained time watermark never moves backwards. If a later leader supplies
a lower wall time, the state machine uses the existing watermark. A newly
appended record uses that same effective time, preventing clock regression
from deleting a just-committed record differently during replay.

### Offsets, deduplication, and consumer checkpoints

Retention advances `base_offset` but never renumbers retained records or
changes `next_offset`. Fetch below the base remains a typed conflict.

Profile-level deduplication entries are removed with their retained record.
Consensus-level exact retry remains independently available for the bounded
proposal suffix carried by EPSN v2 checkpoints.

A consumer checkpoint below `base_offset` is preserved rather than silently
rewritten. Lag counts only retained readable records and observations set
`checkpoint_out_of_range: true`. Group fetch fails at the retained-range
boundary until the caller performs an explicit generation-fenced reset to a
valid offset.

### Read, receipt, and recovery evidence

The direct tablet routes are:

- `PUT /experimental/v1/tablets/stream/retention`;
- `POST /experimental/v1/tablets/stream/retention/maintenance`;
- `GET /experimental/v1/tablets/stream/retention`.

The authenticated regional v1 router exposes the same suffixes. Regional
retention reads default to a quorum-confirmed linearizable barrier.

Mutation receipts include the effective policy, cutoff, previous and new base,
end offset, removed and retained record counts, canonical bytes, command time,
write evidence, and replay disposition. The native Stream snapshot stores the
policy, retained records, base offset, group checkpoints, deduplication state,
and optional age watermark. Decode verifies count, byte, age, offset, and
canonical-encoding invariants before installation.

## Consequences

- Replica-local wall clocks never independently delete Stream data.
- Time, size, and combined policies converge under replay, failover, checkpoint
  installation, and all-voter restart.
- The exact byte definition is stable and testable across machines.
- Consumers can distinguish an empty in-range result from retention data loss.
- Idle age deletion requires a committed maintenance call until managed
  scheduling is implemented.

## Rejected alternatives

- EPRS segment rotation as retention: rotation preserves every logical frame.
- Local read-time filtering: voters could disagree and base offsets would not
  advance durably.
- In-memory object size: allocator, architecture, and implementation details
  would change the contract.
- Silent checkpoint clamping: it would hide data loss and weaken explicit reset
  and ownership semantics.
- Reusing v1, v2, or v3 command versions: it would break their golden byte and
  operation-kind contracts.

## Verification

Required evidence includes:

- exact age-boundary, byte, combined, oversize, dedupe, stale-checkpoint, and
  snapshot tests in `epoch-stream`;
- canonical v4 golden bytes, version/kind rejection, deterministic digest,
  exact replay, and native snapshot tests in `epoch-tablet`;
- strict HTTP parsing, bounds, typed receipts, three-real-runtime convergence,
  checkpoint, and restart in `epoch-node`;
- Go, Java, and Python request/validation tests plus executable docs examples;
- the authenticated regional Docker campaign configuring, maintaining,
  observing, failing over, and reopening the retained boundary.

## Non-claims

This decision does not provide keyed compaction, tombstones, delete retention,
object-tier retention, legal hold, namespace policy guardrails, dynamic
partition expansion/remapping, resource-wide policy coordination, or a managed
periodic maintenance scheduler. Those remain
separate requirements and must not be inferred from a v4 retention receipt.
