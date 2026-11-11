# EpochFlow Agent Guidelines

Rules apply to humans and AI agents. A nested `AGENTS.md` overrides these rules
inside its directory.

## Commits

Make commits small, coherent, independently reviewable, and safe to revert.

- One purpose per commit; do not mix features, refactors, formatting, generated
  files, or unrelated documentation.
- Prefer logical checkpoints: interface/schema, implementation, tests, then
  documentation. Keep behavior tests with the behavior when practical.
- Never commit known build/test failures, secrets, local environment files, or
  unrelated workspace changes.
- On a new calendar day, inspect prior-day uncommitted work. If it is a working
  coherent unit, validate and commit it before unrelated work. Do not commit
  broken work just to satisfy this rule; first make a safe checkpoint or record
  the blocker, and never absorb unrelated changes.

Use Conventional Commits:

```text
<type>(<optional-scope>): <imperative summary>
```

| Type | Meaning |
| --- | --- |
| `feat` | New capability |
| `fix` | Behavior correction |
| `docs` | Documentation only |
| `test` | Tests only |
| `refactor` | Behavior-preserving restructuring |
| `perf` | Performance improvement |
| `build` | Build, dependencies, Docker, packaging |
| `ci` | CI automation |
| `chore` | Other maintenance |
| `style` | Formatting only |

Use concise lowercase scopes when useful: `coordinator`, `worker`, `storage`,
`grpc`, `lease`, `faults`, `docs`. Example:

```text
feat(coordinator): assign pending jobs with durable leases
fix(storage): reject completion from stale generation
docs(architecture): document fencing-token invariant
```

Mark breaking changes with `!` and explain migration in the body:
`feat(api)!: replace polling endpoint with worker stream`.

## Scope and simplicity

Build the smallest solution that meets current requirements, acceptance criteria,
and correctness invariants. Do not add speculative abstractions, configuration,
services, dependencies, or scaling guarantees. Prefer direct code, the standard
library, and existing project patterns. Document a limitation and upgrade trigger
instead of prematurely solving it.

Do not simplify away EpochFlow's essential guarantees: durable state, atomic
state transitions, correct leases, fencing, and idempotency where side effects
need it. Record the trade-off for deliberately simple designs.

## Test-first implementation

Before implementing a component or behavior change, write its tests first. Tests
must state the intended observable behavior and applicable failure cases; only
then write the smallest implementation that makes them pass. Refactor only while
the test suite remains green.

For distributed behavior, include the relevant safety or recovery case (such as
lease expiry, stale generation, duplicate request, crash, or timeout), not only
the happy path. A test may be deferred only when it is genuinely impractical;
document the reason and the planned verification instead.

## Documentation is part of done

Update documentation in the same change set whenever behavior affects architecture
or data flow; public APIs/protobuf/schema/config/commands; job state, recovery,
retry, or failure semantics; invariants, transactions, leases, or fencing; setup,
deployment, metrics, or fault injection; or notable limitations and risks.

Use comments for intent and constraints, never obvious syntax. Public classes,
functions, RPCs, config keys, and database fields need semantics, ownership,
errors, and concurrency notes where relevant. Put non-obvious invariants next to
their enforcement code. For state changes and external effects, state prior and
resulting state, atomicity, idempotency/fencing, retry policy, and ambiguous
outcome handling.

Keep durable documentation under `docs/` as needed:

```text
README.md                  # Purpose, quick start, document navigation
docs/architecture.md       # Components and data/control flows
docs/invariants.md         # Safety properties and enforcement
docs/protocol.md           # gRPC/protobuf semantics
docs/storage.md            # Schema, transactions, migrations
docs/operations.md         # Config, runbook, metrics, troubleshooting
docs/testing.md            # Test and fault-injection strategy
docs/decisions/            # ADRs
```

Link pages from `README.md` or `docs/architecture.md`. A substantive technical
page uses these sections when applicable:

```markdown
# <Topic>
## Purpose
## Context and terminology
## Design
## Invariants and failure semantics
## Implementation map
## Operating and testing
## Trade-offs and limitations
## Related documents
```

Short how-to pages use: **Purpose**, **Prerequisites**, **Steps**,
**Verification**, **Troubleshooting**, and **Related documents**.

Create an ADR for decisions affecting correctness, compatibility, or meaningful
future cost (for example lease policy, fencing, state model, retry guarantees,
storage, or external effects). Name it
`docs/decisions/0001-short-decision-name.md` and include: **Status**, **Context**,
**Decision**, **Consequences**, **Alternatives considered**, and **References**.

## AI context efficiency

Read only relevant files and narrow command output. After a substantial milestone,
context switch, or long investigation, create a concise handoff before compaction:
goal, decisions, changed files, affected invariants, validation, current status,
and next step. Reuse that handoff rather than replaying history. Be concise, but
never skip validation, requirements, or safety reasoning to save tokens.

## Before finishing

- Changes are focused and use the commit rules.
- Success and relevant failure behavior are tested.
- Docs, invariants, failure semantics, and implementation map are current.
- Consequential decisions have an ADR.
