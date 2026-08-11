# ADR-0022: Profile-native checkpoints and physical EPRS reclamation

- Status: Accepted and implemented in the fixed-voter replicated core
- Date: 2026-08-11
- Owners: Rust profile, node, consensus, and storage
- Requirement links: G2, G3, M2 restore exit; CACHE-008 and MGD-006 prerequisites

## Context

ADR-0021 established durable EPSN v1 consensus checkpoints, logical Raft-prefix
compaction, snapshot catch-up, and checkpoint-plus-tail reopen. That slice is
safe but intentionally retains two unbounded histories:

- EPSN v1 contains every unique committed proposal so a profile can be rebuilt
  by replay and every proposal ID remains an exact retry forever; and
- the outer single-file EPRS journal retains every earlier stable generation
  even after the latest checkpoint makes those generations logically obsolete.

Consequently, checkpoint creation eventually reaches the 768 KiB v1 limit,
restart time remains proportional to command history, and disk use does not
fall after compaction. It also does not exercise restoration from a native
Catalog, Stream, Queue, Cache, or Event Bus state image.

## Decision

Epoch will add an additive **EPSN v2 profile checkpoint** and an additive
**EPRS compacted-baseline record**. V1 images and EPRS kinds 1 through 3 remain
readable without reinterpretation.

### Profile image boundary

The consensus crate owns a profile-neutral `ApplicationSnapshot` envelope. It
binds one opaque 16-byte format identifier and schema version to:

- the consensus applied index at which the image was captured;
- a profile state digest;
- a SHA-256 digest of the canonical payload; and
- the bounded canonical payload bytes.

Each profile owns its payload schema and validates its complete scope and
configuration during install. Catalog validates group and epoch. Tablet
profiles validate group, group epoch, tablet ID, tablet epoch, and immutable
profile configuration. Unknown formats, versions, fields, noncanonical bytes,
scope mismatch, digest mismatch, and invalid domain invariants fail closed.

Profile capture and installation extend `CommittedProposalApplier`; they do
not move profile serialization into consensus. The consensus actor is the
consistency boundary. It serializes command application and checkpoint work,
captures the profile only after all commits through the target index have been
applied, and supplies that same index to the adapter.

### Bounded exact-retry contract

EPSN v2 replaces the complete proposal history with the newest exact-retry
suffix that fits both limits:

- at most 1,024 unique committed proposals; and
- at most 1 MiB of encoded retry records.

The most recent proposal must fit or checkpoint creation fails. Entries are
retained in increasing commit-index order. Within the retained suffix, exact
retry and conflicting proposal reuse behave exactly as before. An older
proposal ID is `unknown` at the consensus lookup boundary after it ages out;
it is not reported as committed. Product APIs must document their own
idempotency horizon and must not claim lifetime consensus deduplication.

The profile snapshot retains the matching bounded applied-result suffix so an
accepted recent retry can return its original typed receipt. Live business
state is retained independently of that retry suffix.

### Consensus digest transition

EPSN v1 keeps EPDG v1 unchanged. EPSN v2 uses EPDG v2, a rolling SHA-256 chain
initialized with group and epoch and advanced once for every unique committed
proposal using its log index, term, proposal ID, payload length, and bytes.
The v2 image stores the final chain digest and total unique-command count.
This lets later committed tails advance and validate the digest without the
discarded prefix. The first v2 checkpoint computes the chain from the complete
v1 history before discarding it.

### Capture, persistence, and installation order

Local creation follows this order:

1. Drain current Raft Ready work on the actor.
2. Capture and canonically validate the profile image at the current applied
   index without mutating profile state.
3. Build and validate EPSN v2, including the EPDG v2 chain and retry suffix.
4. Append and fsync the ordinary EPRS checkpoint transition.
5. Replace the physical EPRS WAL with a complete identity plus compacted
   baseline and fsync the parent directory.
6. Install the logical Raft checkpoint in memory.
7. Return success with the existing voter-local checkpoint and retained-index
   observations plus the explicit physical-EPRS compaction mode.

A follower installs in the corresponding durable-first order. Consensus first
persists the received image and Raft state, then the actor replaces the typed
profile state, then applies only committed entries after the image index, and
only then publishes receipts or readiness.

An error after any durable write fail-stops the live adapter. Reopen selects a
complete old or new journal and repeats profile installation before becoming
ready. It never guesses whether a partial in-memory transition succeeded.

### Crash-safe physical replacement

The current `FileWal` path remains the exclusive ownership boundary. Physical
reclamation creates a uniquely named sibling WAL, exclusively locks it, writes
identity at physical sequence zero and one EPRS compacted baseline at sequence
one, fsyncs it, atomically renames it over the active path while both old and
new inodes are locked, and fsyncs the parent directory. Only then is the old
locked descriptor released.

Before rename, reopen sees the complete old journal. After rename, reopen sees
the complete compacted journal. If the directory fsync outcome is unknown,
the actor fail-stops; either crash outcome is valid because the old journal
already contains the same durable checkpoint. Temporary siblings are never
selected as authoritative state and are removed only while the active path is
exclusively owned.

EPRS kind 4 identifies the compacted baseline. It is valid only as the first
record after identity. Its embedded logical stable generation may be greater
than physical outer sequence one. Later records advance both their physical
sequence and the independent logical generation contiguously. This distinction
prevents a missing middle record in a legacy journal from being mistaken for a
valid compacted history.

## Required evidence

The feature is not complete until tests prove:

1. canonical round-trip, scope/config fencing, digest corruption rejection,
   unknown-version rejection, and bounded-retry trimming for every profile;
2. EPSN v1 compatibility plus exact EPSN v2 codec, EPDG v2, size, retry, and
   malformed-image tests;
3. v2 checkpoint-plus-tail reopen without replaying the discarded prefix;
4. exact recent retry preservation and truthful expiry of an aged-out ID;
5. physical file-size reduction and continued generation advancement after
   compacted-baseline reopen;
6. old-path, new-path, orphan-temporary, post-rename-unknown, corruption, and
   writer-exclusion replacement histories;
7. the profile-neutral lagging-voter path installs a native snapshot before its
   tail, while Catalog, Stream, Queue, Cache, and Event Bus each prove native
   capture/install plus automatic real-runtime restart from a forced
   checkpoint; and
8. real-process restart plus the complete local and CI regression gates.

## Consequences and non-claims

This decision makes restart and consensus retry metadata independent of total
command history and reclaims obsolete EPRS generations. It exercises native
profile restore in the fixed-voter runtime.

It does **not** claim managed backup, a user-downloadable snapshot, PITR,
remote-tier upload, encryption, multi-file snapshot chunking, dynamic
membership, online tablet movement, automated repair, or cross-version restore
orchestration. EPSN v2 remains a bounded internal Raft snapshot transported in
one peer frame. Those are later G2, G3, G8, CACHE-008, and MGD-006 deliverables.
