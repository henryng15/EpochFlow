# Architecture

## Purpose

Describes EpochFlow's components and their data and control flows. This page
tracks what exists; planned parts are marked as such.

## Context and terminology

- **Job:** a unit of work with a payload, a lifecycle state, and a result.
- **Coordinator:** assigns jobs, grants leases, and decides recovery.
- **Worker:** acquires a job, renews its lease, executes the job, and reports a
  result.
- **Lease:** time-limited permission for one worker to execute one job.
- **Generation:** a per-job counter incremented on every assignment; it is the
  fencing token that rejects requests from stale assignments.
- **Reclaim:** recovery of jobs whose lease deadline has passed.

## Design

Current status: **milestone 4 (idempotency, UNKNOWN, fault injection)**. A
coordinator process serves the gRPC API over PostgreSQL (`--dsn`) or, without
it, an in-memory store; worker processes pull jobs and run demonstration tasks
whose side effects are deduplicated (owned ledger) or guarded against blind
repetition (external provider). The in-process `LocalRunner` remains for tests
and runs the same `Worker` code.

### Job lifecycle

```text
             acquire (generation + 1)
  PENDING ─────────────────────────────▶ RUNNING ──complete──▶ COMPLETED
     ▲                                     │ │
     │ retryable failure or expired lease, │ │ non-retryable failure, or
     └─────────── attempts left ───────────┘ │ no attempts left
                                             ├────────────────▶ FAILED
                                             │
                                             └─ unconfirmed ──▶ UNKNOWN ──reconcile──▶ COMPLETED | FAILED
                                                external effect          (operator)
```

Rules and their rationale: [ADR 0002](decisions/0002-job-state-model-and-fencing-rules.md).

### Processes and data flow

```text
epochflowctl ──SubmitJob / GetJob──┐
                                   ▼
                     epochflow-coordinator
                      ├─ CoordinatorService ── one atomic call per RPC ──▶ JobStore
                      └─ Reclaimer ── ReclaimExpired() every interval ───▶ PgJobStore
                                   ▲                                       (PostgreSQL), or
                                   │                                       InMemoryJobStore
     AcquireJob / RenewLease /     │ gRPC (plaintext)
     CompleteJob / FailJob         │
                     epochflow-worker × N
                      └─ Worker over CoordinatorClient (a WorkSource)
                          ├─ LeaseKeeper thread: RenewLease every lease/3
                          ├─ task(assignment, lease_lost token)
                          └─ report with generation, retried if the ack is lost
```

The store makes every change atomically and enforces fencing; the coordinator
service, workers, and runner hold no authoritative state, so a coordinator can
crash and restart without losing jobs. In PostgreSQL each change is one
conditional statement and the database clock decides lease expiry. Details:
[protocol.md](protocol.md), [storage.md](storage.md).

### Side effects

```text
payment task                         external task
  ApplyLedgerOperation(op)             BeginExternalOperation(op)
    └ store, one transaction:            ├ START     → provider.Charge → ConfirmExternalOperation
      fence + operation + entry          ├ CONFIRMED → reuse the earlier answer
      (repeat op → same entry)           └ IN_DOUBT  → MarkJobUnknown (never call again)
                                       provider timeout → MarkJobUnknown
```

An `operation_id` (payload `op`, default `job-<id>`) names one logical effect
across attempts and duplicate jobs. Details:
[ADR 0005](decisions/0005-idempotent-operations-and-unknown-outcomes.md).

### In-process mode

`LocalRunner` wires N `Worker`s to the store through `StoreWorkSource` and runs
one `Reclaimer`: the same components without gRPC.

## Invariants and failure semantics

See [invariants.md](invariants.md).

| Failure | Behavior |
| --- | --- |
| Handler throws | Retryable failure; re-queued while attempts remain |
| Renewal rejected or errors (including RPC failure) | Handler is cancelled via its stop token; result discarded |
| Result report fails in transport | Retried with the same generation (idempotent); if all tries fail, lease expiry recovers the job |
| Worker killed (SIGKILL) | Lease expires, job reclaimed and re-run with a new generation |
| Worker crashes after an owned effect, before reporting | Re-run deduplicates: the original ledger entry is returned |
| Worker paused, resumes after reassignment | Its effect and its report are rejected as stale; nothing changes |
| Same logical request submitted twice | One effect; the second job reports the deduplicated entry |
| `operation_id` reused with different content | Job fails without retry (conflict) |
| External provider times out | Job `UNKNOWN`; not retried until an operator reconciles |
| Worker crashes during an external call | Next attempt finds the unconfirmed intent and reports `UNKNOWN` instead of charging again |
| Coordinator crash and restart (PostgreSQL) | All committed state survives; leases keep their deadlines; expired ones are reclaimed after restart |
| Coordinator restart (in-memory mode) | All state lost |
| PostgreSQL unavailable | RPCs fail with `UNAVAILABLE`; no state changes; work resumes when it returns |
| Database connection lost | Outcome of the in-flight call unknown (`UNAVAILABLE`); the pool reconnects |
| Worker stops renewing (crash, pause) | Reclaim re-queues the job after the deadline; the old generation is rejected afterwards |
| Result arrives after the deadline, before reclaim | Accepted (no newer assignment exists) |
| Duplicate result for the same generation | Same content: accepted, no change. Different: conflict, no change |
| Store throws during a worker step | Worker backs off; any held lease expires and is reclaimed |

## Implementation map

| Path | Contents |
| --- | --- |
| `src/core/job_state.*` | States and transition table |
| `src/core/job.*`, `src/core/clock.h` | Records, assignment, commit status, time types |
| `src/core/job_store.h` | `JobStore` contract (atomicity, fencing, errors) |
| `src/store/in_memory_job_store.*` | Mutex-guarded reference store |
| `src/store/pg_job_store.*`, `src/store/pg_connection_pool.*` | PostgreSQL store and connection pool |
| `src/store/migrations.*`, `sql/migrations/` | Schema and embedded migrations |
| `src/worker/lease_keeper.*` | Background lease renewal with lost-lease signal |
| `src/worker/work_source.h`, `src/worker/worker.*` | Worker loop over a `WorkSource`; report retries |
| `src/coordinator/reclaimer.*` | Periodic expired-lease recovery |
| `src/coordinator/coordinator*.*` | gRPC service and server |
| `src/client/coordinator_client.*` | gRPC client (`WorkSource`) |
| `src/rpc/convert.*` | Wire conversion and status mapping |
| `src/worker/tasks.*`, `src/worker/external_provider.*` | Demonstration tasks and the simulated provider |
| `src/runner/local_runner.*` | In-process workers and reclaimer |
| `scripts/check-invariants.sql` | SQL invariant checker |
| `apps/` | `epochflow-coordinator`, `epochflow-worker`, `epochflowctl` |
| `proto/epochflow/v1/` | Protocol schema |
| `tests/contract/` | Behavioral suite every `JobStore` must pass |

## Trade-offs and limitations

- The in-memory store is not durable; use `--dsn` for anything beyond tests.
- One coordinator; no coordinator replication in v1.
- Plaintext, unauthenticated gRPC (see [protocol.md](protocol.md)).
- PostgreSQL unavailability blocks all state transitions (by design in v1).
- `LocalRunner::Stop` and worker shutdown wait for the in-flight job; a task
  that ignores its stop token can delay shutdown indefinitely.

## Related documents

- [Invariants](invariants.md)
- [Testing](testing.md)
- [Protocol](protocol.md)
- [Storage](storage.md)
- [Operations](operations.md)
- [ADR 0001: toolchain and dependencies](decisions/0001-cpp-toolchain-and-dependencies.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
- [ADR 0003: coordinator protocol](decisions/0003-coordinator-protocol.md)
- [ADR 0004: PostgreSQL job store](decisions/0004-postgresql-job-store.md)
- [ADR 0005: idempotent operations and UNKNOWN outcomes](decisions/0005-idempotent-operations-and-unknown-outcomes.md)
