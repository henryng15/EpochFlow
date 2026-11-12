# Phase 2: confirmation correctness and evidence plan

## Purpose

Phase 2 closes two correctness gaps in the Version 1 external-effect path, then
turns the existing one-seed chaos demonstration into repeatable performance
evidence. It does **not** add Kubernetes, consensus, autoscaling, a workflow
DSL, or a new AI scheduler.

This page is a plan, not a statement that the work has been implemented. Version
1 remains the current released behavior. See [Architecture](architecture.md)
and [ADR 0005](decisions/0005-idempotent-operations-and-unknown-outcomes.md)
for the current external-effect design.

## Context and terminology

- **External operation:** a provider-side effect that EpochFlow does not own,
  such as a charge. EpochFlow records its intent before calling the provider.
- **Confirmation:** the durable `operations` record changes from `STARTED` to
  `CONFIRMED` after EpochFlow has provider evidence of the effect.
- **Reconciliation:** an operator resolves an `UNKNOWN` job using independent
  provider evidence.
- **Unknown:** the provider may have performed an effect, but EpochFlow cannot
  safely prove its outcome. It is not a retryable failure.

## Design

### Goals and order

| Priority | Work item | Outcome |
| --- | --- | --- |
| P0 | Make external confirmation mandatory before reporting success | A provider charge cannot be reported as a confirmed job completion when durable confirmation is missing or conflicting. |
| P1 | Reconcile the job and its external operation atomically | A successful operator reconciliation lets later attempts reuse the provider-confirmed result rather than remaining `IN_DOUBT`. |
| P1 | Produce multi-run benchmark evidence | Report correctness and performance over many seeds, worker counts, and job counts—not one favourable sample. |
| P2 | Execution decision trace | Make recovery/rejection decisions easy to explain during demos and debugging. |
| P2 | AI document-processing example | Demonstrate an application of the engine without changing its safety model. |

Phase 2 starts with P0 only. P1 work begins after its P0 regression tests pass.
P2 items are optional portfolio polish, not prerequisites for calling Phase 2
complete.

### P0: external confirmation must be authoritative

Current Version 1 behavior retries `ConfirmExternal` after a provider charge,
but does not inspect its returned `CommitStatus`; after three thrown transport
errors it can still return job success. Therefore the job may become
`COMPLETED` while the operation remains `STARTED`.

Phase 2 rule:

```text
provider charge succeeds
  -> durable confirmation accepted (including an identical duplicate) -> COMPLETED
  -> confirmation conflict, rejection, or exhausted transport retries  -> UNKNOWN
```

The worker must never call the provider again after an ambiguous confirmation.
It must return an outcome that causes the coordinator to park the job as
`UNKNOWN`, preserving the existing no-blind-retry rule. A stale assignment must
still be rejected rather than being treated as a successful confirmation.

Required tests, written before implementation:

1. A successful provider charge plus `ConfirmExternal = kAccepted` completes
   and stores a confirmed operation.
2. A confirmation conflict does not report success and does not invoke the
   provider again.
3. Three confirmation transport failures produce `UNKNOWN`, not `COMPLETED`.
4. A lost confirmation acknowledgement that was durably applied is recognized
   as the same confirmation and safely completes.
5. The PostgreSQL path proves no completed external job is paired with an
   unresolved `STARTED` operation after the worker has reported success.

### P1: operation-level reconciliation

`Reconcile` currently resolves only `jobs`. For an external job, an operator
may establish that a provider charge succeeded and set the job to `COMPLETED`,
while its operation remains `STARTED`. A later use of the same operation ID
then remains `IN_DOUBT` even though evidence is available.

Before implementation, create an ADR that chooses the API and schema contract.
The preferred direction is a dedicated operation-reconciliation command that,
in one PostgreSQL transaction:

1. verifies the job is `UNKNOWN` and ties it to the specified external operation;
2. records provider evidence and transitions the operation to `CONFIRMED` when
   the operator establishes success;
3. transitions the job to `COMPLETED` with the same evidence; and
4. makes a repeated identical decision idempotent while rejecting conflicting
   evidence.

The ADR must also define the negative-evidence path: whether an operation
proven not to have occurred receives a durable terminal operation state, and
what a future request with that ID returns. Do not silently conflate this case
with an unresolved `STARTED` intent.

Extend the invariant checker only after that contract exists. At minimum it
must detect an external job reconciled to `COMPLETED` whose associated operation
is not durably confirmed, and an impossible operation/job evidence mismatch.

Required tests, written before implementation:

1. Reconciliation with successful provider evidence updates job and operation
   atomically.
2. A later job using that operation ID reuses the confirmed provider result.
3. Repeating the same reconciliation is idempotent; conflicting evidence is
   rejected without changing either record.
4. A crash or transaction failure leaves both records at their previous
   consistent state.
5. The invariant checker reports deliberately injected job/operation divergence.

### P1: benchmark matrix

Retain the paired EpochFlow-versus-naive comparison: each pair must use the
same deterministic job set and chaos schedule. Run at least 20 seeds for each
matrix cell, with worker counts `1`, `3`, and `6`, and job counts `200` and
`1000` (120 paired runs total). If local capacity makes the full matrix too
slow, record the reduced matrix and reason rather than treating a subset as
equivalent evidence.

For every run record raw machine-readable output and publish aggregate median,
p50, p95, minimum, and maximum for:

- throughput;
- recovery latency;
- retries and stale requests rejected;
- effects deduplicated, duplicate ledger entries, and overcharge; and
- invariant violations and incomplete jobs.

Report hardware/OS, PostgreSQL version, worker/job parameters, lease settings,
chaos settings, seed range, and the exact command. Do not claim a statistically
general performance result outside this local environment.

### P2: deferred polish

An execution decision trace should explain assignment, renewal, reclaim,
fencing rejection, idempotency decision, and reconciliation. Start with a
structured, queryable event log; do not build a web UI until the log answers
real debugging questions.

The AI example should be a small document-processing pipeline that demonstrates
checkpointed jobs and an ambiguous external model call. It must preserve the
same `UNKNOWN` semantics; it must not claim idempotent inference or billing
without provider support.

## Invariants and failure semantics

Existing fencing and idempotency invariants remain unchanged. Phase 2 adds this
proposed invariant:

| ID | Proposed invariant | Enforcement target |
| --- | --- | --- |
| P2-I1 | A worker must not report a provider effect as completed unless its durable operation record confirms the same operation ID, request, and provider result. | Task outcome handling, operation transaction, PostgreSQL invariant checker |

When confirmation cannot be established, the safe terminal state is `UNKNOWN`.
It requires reconciliation; it is neither retried automatically nor converted
to `FAILED` merely because confirmation transport failed.

## Implementation map

No code changes are made by this plan. Expected implementation locations are:

| Area | Likely files |
| --- | --- |
| P0 task outcome policy | `src/worker/tasks.cpp`, `tests/unit/tasks_test.cpp` |
| Operation contract/store | `src/core/job_store.h`, `src/store/in_memory_job_store.*`, `src/store/pg_job_store.*`, `tests/contract/job_store_contract.cpp` |
| Reconciliation API | `proto/epochflow/v1/epochflow.proto`, coordinator/client/apps, gRPC tests |
| Schema and checks | a new `sql/migrations/` file, `scripts/check-invariants.sql`, PostgreSQL integration tests |
| Benchmark results | `scripts/bench.py`, documented result artifact and `README.md` |
| Decision trace, if approved | new focused trace module and fault tests |

## Operating and testing

Each P0/P1 change follows test-first development. Run the focused regression
test first, then the full suite with PostgreSQL and the ThreadSanitizer unit
suite:

```bash
scripts/test-db.sh up
EPOCHFLOW_DSN="$(scripts/test-db.sh dsn)" ctest --preset dev --output-on-failure
setarch "$(uname -m)" -R ctest --test-dir build-tsan -L unit --output-on-failure
scripts/test-db.sh down
```

The P1 benchmark must use a disposable test database and remove per-run schemas
after collection. Preserve raw benchmark output separately from its aggregate
summary so a reviewer can reproduce the calculation.

## Trade-offs and limitations

- P0 may leave more jobs in `UNKNOWN`; this is intentional. Ambiguity is safer
  and more honest than declaring an unconfirmed external effect successful.
- Atomic job/operation reconciliation is a PostgreSQL guarantee for records
  EpochFlow owns. It does not prove a third-party provider's state without the
  operator evidence supplied to reconciliation.
- The performance matrix measures a local prototype. It does not establish
  production throughput, cloud-network latency, or provider performance.
- TLS and authentication remain outside Phase 2 while the engine is a local
  prototype. They become required before exposing gRPC beyond a trusted network.

## Related documents

- [Architecture](architecture.md)
- [Invariants](invariants.md)
- [Protocol](protocol.md)
- [Storage](storage.md)
- [Testing](testing.md)
- [Operations](operations.md)
- [ADR 0005: idempotent operations and UNKNOWN outcomes](decisions/0005-idempotent-operations-and-unknown-outcomes.md)
