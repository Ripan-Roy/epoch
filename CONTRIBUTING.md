# Contributing to Epoch

Thanks for helping build Epoch. This repository is an early distributed-systems
project: interfaces and storage formats can still change, but correctness and
honest guarantees are required in every change.

By contributing, you agree that your contribution is licensed under the
[MIT License](LICENSE).

## Start with the product contract

Read the [PRD](docs/PRD.md), [architecture](docs/ARCHITECTURE.md),
[semantics](docs/SEMANTICS.md), and
[engineering standards](docs/ENGINEERING_STANDARDS.md) before changing runtime
behavior. Delivery claims must be reflected in the
[requirements traceability matrix](docs/REQUIREMENTS_TRACEABILITY.md) and
[delivery checklist](docs/DELIVERY_CHECKLIST.md).

Keep these boundaries intact:

- Rust owns latency-critical data paths, persisted formats, and replication.
- Go owns hosted control and fleet-management loops; it must not read Epoch
  storage files or mutate profile state directly.
- TypeScript/React owns the web console and uses the Go browser API rather than
  contacting storage nodes directly.
- Public and internal contracts are versioned and do not expose implementation
  types as wire formats.

## Development setup

Follow [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md), then verify the pinned tools:

```shell
make bootstrap-check
```

Use the root Make targets as the common interface while debugging failures with
the native Cargo, Go, Maven, Python, or pnpm command printed by the target.

## How changes are developed

Behavioral changes use test-driven development:

1. Add or update a failing test that states the required behavior and failure
   boundary.
2. Implement the smallest cohesive design that makes the test pass.
3. Refactor only with the tests green, preserving SOLID responsibilities and
   explicit dependency direction.
4. Add integration, recovery, compatibility, or fault evidence in proportion
   to the guarantee being changed.
5. Update user documentation and traceability in the same pull request.

External contributors must not submit large feature pull requests. Contributions
should be small, issue-scoped, and easy to review. Do not start a broad feature,
cross-stack rewrite, public contract change, or persisted-format migration
without a maintainer-created issue that defines a contributor-sized task. Large
feature releases spanning the data plane, control plane, SDKs, console, and
documentation are coordinated and assembled by maintainers.

Clean-code expectations include descriptive names, small ownership boundaries,
validated inputs, bounded allocations and queues, deterministic state-machine
behavior, typed errors, no hidden guarantee upgrades or downgrades, and no
duplicated business rules across layers. Persisted and public format changes
need an explicit compatibility plan and pinned tests.

## Local verification

Run the focused tests continuously. Before opening a pull request, run:

```shell
make format-check
make lint
make test
make build
```

For runtime, protocol, SDK, persistence, replication, failover, or deployment
changes, also run the relevant integration gate:

```shell
make test-integration
```

`make check` is the normal pre-commit gate and `make ci` mirrors the
deterministic CI suite. Docker-backed campaigns may take longer and are
required when a pull request claims process, network, disk, failover, recovery,
or published quickstart behavior.

Do not weaken, skip, or delete a gate simply to make CI pass. If a test is
flaky, fix its synchronization or deterministic harness and document the root
cause.

## Pull requests

External contributors must keep each pull request focused on one small,
maintainer-scoped issue. Maintainer-owned feature releases may deliberately
span implementation, SDK, integration, console, and documentation layers when
those changes form one complete product capability. In either case, avoid
drive-by refactors and unrelated cleanup. The description should include:

- the problem and delivered behavior;
- the exact guarantees, limits, and non-claims;
- architecture or format decisions;
- tests and local commands run;
- compatibility, migration, security, and operational impact;
- documentation and PRD/traceability rows changed.

All required CI, formatting, lint, unit, integration, and documentation checks
must pass before merge. Review feedback should be resolved in the same branch,
and generated files must be regenerated from their source rather than edited
manually.

## Security and sensitive data

Do not open a public issue for a suspected vulnerability. Follow
[docs/SECURITY.md](docs/SECURITY.md). Never commit credentials, production data,
private endpoints, signing keys, tokens, or customer-identifying fixtures.

## Documentation

Public behavior is incomplete until its API examples, SDK guidance, operational
limits, failure modes, release notes, and traceability evidence are current.
Use precise language: an engineering prototype, simulated fault, or stronger
local result is not production, multi-zone, geo, or exactly-once evidence.
