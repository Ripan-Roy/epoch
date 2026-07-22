# Engineering Standards

**Status:** Required for all production-bound changes
**Applies to:** Rust, Go, Python, TypeScript, Protobuf, deployment code, and documentation

Epoch is correctness-sensitive infrastructure. A change is not complete because
it compiles or demonstrates a happy path; it is complete when its behavior,
failure mode, architectural boundary, and verification evidence agree.

## 1. Test-driven development

Behavior changes follow red → green → refactor:

1. Add the smallest test that describes the required externally observable
   behavior or invariant.
2. Run it and confirm that it fails for the intended reason.
3. Implement the smallest coherent change that makes it pass.
4. Run the focused test, then the owning package suite, then the repository
   gate.
5. Refactor names, duplication, and boundaries while tests remain green.

Bug fixes require a regression test that fails before the fix. Distributed
correctness work additionally requires deterministic fault/history tests; a
happy-path unit test is not sufficient evidence for durability, fencing,
ordering, settlement, or recovery.

Tests must be deterministic. Engine code receives an injected clock and fault
source. Tests must not depend on sleeps, wall-clock timing, unordered map
iteration, public internet services, or shared mutable environments.

## 2. SOLID and dependency rules

- **Single responsibility:** domain types, storage, profile state machines,
  protocols, orchestration, and UI remain separate ownership boundaries.
- **Open/closed:** new protocols and persistence backends implement narrow
  adapters; they do not add protocol branches throughout profile engines.
- **Liskov substitution:** every adapter must pass the same contract suite as
  the implementation it replaces. A mock cannot expose guarantees that its
  production counterpart lacks.
- **Interface segregation:** traits and Go interfaces are consumer-owned and
  minimal. Avoid universal broker, storage, or control-plane interfaces.
- **Dependency inversion:** protocol and management layers depend on typed
  engine/admin contracts. Engines depend on clocks, logs, consensus, and key
  providers through narrow abstractions—not concrete gateways or hosted Go
  services.

The enforced direction is:

```text
domain + time
      ↓
storage + consensus
      ↓
tablet/profile state machines
      ↓
protocol adapters + node

hosted Go control ──versioned admin contract──▶ regional Rust authority
```

Profile engines do not import one another. Bus-to-Queue or Stream-to-Queue
movement is an explicit pipe/route with its own state and receipt.

## 3. Clean-code rules

- Prefer names that state the semantic boundary: `commit_position`,
  `lease_generation`, and `achieved_durability` over generic `id` or `state`.
- Keep functions focused; extract policy and state transitions before nesting
  becomes difficult to audit.
- Make invalid states unrepresentable with enums and typed request/receipt
  structures where practical.
- Return typed errors. Retry, conflict, fencing, unknown outcome, and capacity
  are never inferred from message text.
- Never silently downgrade durability, ordering, delivery, authorization, or
  encryption.
- Do not hide I/O, network calls, wall-clock reads, or global mutable state in
  domain logic.
- Comments explain invariants and reasons, not line-by-line mechanics.
- Remove dead code and obsolete compatibility paths in the same change that
  supersedes them.
- Unsafe Rust remains forbidden at workspace level. Any exception requires an
  ADR, documented invariants, isolated scope, fuzz/property tests, and security
  review.

## 4. Required verification by change type

| Change | Minimum evidence |
| --- | --- |
| Pure domain/state transition | Focused unit test plus boundary/property cases |
| HTTP/gRPC/protocol behavior | Contract/integration test through the public surface |
| Storage/recovery | Golden format vector, corruption/partial-write test, restart digest |
| Lease, fencing, transaction, consensus | Deterministic history/fault test and model invariant |
| Bug fix | Reproducing test observed red before implementation |
| UI behavior | Typecheck, lint, production build, error/loading/accessibility path |
| Control-plane mutation | Idempotency, optimistic concurrency, replay, race test |
| Protobuf/API change | Buf lint, generation freshness, compatibility/breaking check |
| Deployment change | Static config validation and smoke/health test |

Coverage percentage is a diagnostic, not proof. Critical invariants and failure
boundaries must be covered even when line coverage is already high.

## 5. Review and definition of done

A change is done only when:

- requirements and acceptance behavior are named;
- tests were added or the change is demonstrably non-behavioral;
- formatters, linters, unit/integration tests, contract checks, and builds pass;
- API, semantics, traceability, runbooks, and examples are updated when affected;
- current behavior is not described as a future guarantee;
- security, tenancy, data-loss, migration, and rollback impact were considered;
- generated artifacts and dependency locks are current;
- the branch is pushed and CI is green.

An exception requires an explicit issue or ADR with owner, scope, risk, and a
time-bounded removal gate. Schedule pressure does not waive correctness,
fencing, recovery, security, or honest guarantee reporting.
