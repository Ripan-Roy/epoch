# ADR-0033: Replicated resource governance and authorized cost attribution

- Status: Accepted
- Date: 2026-08-20

## Context

GOV-005 requires owner, cost center, environment, data classification, and
custom tags. Environment already participates in the immutable resource name,
authorization scope, and placement identity. A second mutable environment field
could drift and make policy decisions ambiguous. The Go registry and Rust
catalog also need one compatible rule for legacy state that predates governance.

Inventory filtering and cost explanation must not aggregate resources the
caller is unauthorized to observe. Governance data must be bounded because it
crosses APIs, durable metadata, consensus commands, snapshots, and the browser.

## Decision

1. A new managed regional resource must provide owner, cost center,
   classification, and optional bounded custom tags. Environment remains solely
   in `ResourceName`/`ResourceKey`.
2. Go canonicalizes governance before durable acceptance. Rust validates the
   same canonical representation before committing catalog command/snapshot
   version 3.
3. Governance is mutable only through the normal expected-generation apply.
   It participates in desired-state equality and request-token fingerprints.
4. Existing valid Go and Rust records without governance remain readable.
   Compatibility applies to recovery, not to new managed creation.
5. Inventory filters use exact AND matching. Cost attribution is computed only
   from the already authorized and filtered resource list, then sorted by cost
   center and classification.
6. Attribution reports resource and desired-shard counts. It is not billing or
   usage metering.

## Consequences

The Go control process and Rust catalog hold the same desired governance value,
so a Go outage does not erase the data-plane copy and a Rust restart does not
depend on Go memory. Canonicalization makes filtering and replay deterministic.
Legacy state can reopen without a forced migration, while all new managed
resources have complete metadata.

The current bootstrap authorization policy does not yet evaluate classification
or tags. Organization guardrails, ABAC, immutable audit export, redaction,
residency, legal hold, real usage meters, rates, and invoices remain separate
requirements.
