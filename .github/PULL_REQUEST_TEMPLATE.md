## Problem and delivered behavior

<!-- Explain the problem, the observable behavior delivered, and why this scope is cohesive. External contributions must be small and tied to a maintainer-scoped issue. -->

Closes #

## Guarantees, limits, and non-claims

<!-- State exact correctness, durability, compatibility, performance, and security claims. Explicitly list what this PR does not claim. -->

## Design and ownership

<!-- Link the relevant ADR, or explain why no architectural/persisted/public boundary changed. Confirm that Rust data plane, Go control plane, SDK, and UI responsibilities remain separated. -->

- ADR: N/A
- PRD / traceability IDs: N/A

## Test-driven evidence

<!-- Name the regression or acceptance test that captured the required behavior before implementation. Include success, rejection, idempotency, fencing, overflow, and restart paths as applicable. -->

## Verification performed

- [ ] Focused tests pass
- [ ] `make format-check` passes
- [ ] `make lint` passes
- [ ] `make test` passes
- [ ] `make build` passes
- [ ] Relevant integration or recovery campaign passes, or is not applicable with a reason
- [ ] Generated contracts are current
- [ ] Dependency and security impact was reviewed

Commands and results:

```text

```

## Compatibility, migration, and operations

<!-- Describe wire/storage/API compatibility, downgrade behavior, rollout or migration steps, resource changes, observability, and failure recovery. Write "No impact" only after checking each boundary. -->

## Documentation and release impact

- [ ] User/API/SDK/operations documentation is updated, or no public behavior changed
- [ ] PRD traceability and delivery checklist are updated when a delivery claim changed
- [ ] Changelog and release notes are updated when user-visible behavior changed
- [ ] Examples contain no secrets, private endpoints, production data, or customer identifiers

## Reviewer focus

<!-- Call out the highest-risk assumptions, invariants, and files reviewers should inspect first. -->
