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

Current status: **milestone 2 (coordinator and gRPC workers)**. A coordinator
process serves the gRPC API over an in-memory store; worker processes pull jobs
from it. The in-process `LocalRunner` remains for tests and runs the same
`Worker` code. Milestone 3 replaces the in-memory store with PostgreSQL.

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

### Processes and data flow (milestone 2)

```text
epochflowctl ──SubmitJob / GetJob──┐
                                   ▼
                     epochflow-coordinator
                      ├─ CoordinatorService ── one atomic call per RPC ──▶ JobStore
                      └─ Reclaimer ── ReclaimExpired() every interval ───▶ (in memory;
                                   ▲                                       PostgreSQL in M3)
     AcquireJob / RenewLease /     │ gRPC (plaintext)
     CompleteJob / FailJob         │
                     epochflow-worker × N
                      └─ Worker over CoordinatorClient (a WorkSource)
                          ├─ LeaseKeeper thread: RenewLease every lease/3
                          ├─ task(assignment, lease_lost token)
                          └─ report with generation, retried if the ack is lost
```

The store makes every change atomically and enforces fencing; the coordinator
service, workers, and runner hold no authoritative state. Protocol details:
[protocol.md](protocol.md).

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
| Coordinator restart | All state lost in milestone 2 (in-memory store) |
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
| `src/worker/work_source.h`, `src/worker/worker.*` | Worker loop over a `WorkSource`; report retries |
| `src/coordinator/reclaimer.*` | Periodic expired-lease recovery |
| `src/coordinator/coordinator*.*` | gRPC service and server |
| `src/client/coordinator_client.*` | gRPC client (`WorkSource`) |
| `src/rpc/convert.*` | Wire conversion and status mapping |
| `src/runner/local_runner.*` | In-process workers and reclaimer |
| `apps/` | `epochflow-coordinator`, `epochflow-worker`, `epochflowctl` |
| `proto/epochflow/v1/` | Protocol schema |
| `tests/contract/` | Behavioral suite every `JobStore` must pass |

## Trade-offs and limitations

- The in-memory store is not durable; restart loses all jobs (milestone 3).
- One coordinator; no coordinator replication in v1.
- Plaintext, unauthenticated gRPC (see [protocol.md](protocol.md)).
- PostgreSQL unavailability will block all state transitions (by design in v1).
- `LocalRunner::Stop` and worker shutdown wait for the in-flight job; a task
  that ignores its stop token can delay shutdown indefinitely.

## Related documents

- [Invariants](invariants.md)
- [Testing](testing.md)
- [Protocol](protocol.md)
- [Operations](operations.md)
- [ADR 0001: toolchain and dependencies](decisions/0001-cpp-toolchain-and-dependencies.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
- [ADR 0003: coordinator protocol](decisions/0003-coordinator-protocol.md)
