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

Current status: **milestone 1 (in-process runner)**. Jobs run on worker threads
inside one process against an in-memory store. Milestone 2 splits this into a
coordinator and gRPC worker processes using the same protocol.

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
                                                external effect          (milestone 4)
```

Rules and their rationale: [ADR 0002](decisions/0002-job-state-model-and-fencing-rules.md).

### Execution flow (milestone 1)

```text
LocalRunner
 ├─ worker thread × N:
 │    Acquire(worker, lease) ──▶ Assignment{job, generation}
 │    LeaseKeeper thread: Renew(job, generation) every renew_interval
 │    handler(assignment, lease_lost token)
 │    Complete / Fail(job, generation)   ← skipped if the lease was lost
 └─ reclaim thread: ReclaimExpired() every reclaim_interval
                       │
                       ▼
                 JobStore (InMemoryJobStore; PostgreSQL in milestone 3)
```

The store makes every change atomically and enforces fencing; workers and the
runner hold no authoritative state.

### Planned distribution (milestones 2 and 3)

```text
Client ──submit──▶ Coordinator ──conditional updates──▶ PostgreSQL
                     ▲      │
        acquire /    │      │ reclaim expired leases
        renew /      │      ▼
        complete ────┴── Workers ──▶ task / side effect
               (gRPC)
```

Workers pull work; lease renewal doubles as the heartbeat. PostgreSQL's clock
will decide lease expiry.

## Invariants and failure semantics

See [invariants.md](invariants.md).

| Failure | Behavior |
| --- | --- |
| Handler throws | Retryable failure; re-queued while attempts remain |
| Renewal rejected or errors | Handler is cancelled via its stop token; result discarded |
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
| `src/worker/lease_keeper.*` | Background lease renewal with lost-lease signal |
| `src/runner/local_runner.*` | Worker threads and reclaim loop |
| `tests/contract/` | Behavioral suite every `JobStore` must pass |

## Trade-offs and limitations

- The in-memory store is not durable; restart loses all jobs (milestone 3).
- One coordinator; no coordinator replication in v1.
- PostgreSQL unavailability will block all state transitions (by design in v1).
- `LocalRunner::Stop` waits for in-flight handlers; a handler that ignores its
  stop token can delay shutdown indefinitely.

## Related documents

- [Invariants](invariants.md)
- [Testing](testing.md)
- [ADR 0001: toolchain and dependencies](decisions/0001-cpp-toolchain-and-dependencies.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
