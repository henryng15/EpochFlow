# EpochFlow

**A fault-tolerant distributed job execution engine for C++ workloads.**

EpochFlow assigns background jobs to independent workers, persists execution
state, and recovers safely when workers crash, pause, or lose network access.
It is a learning-focused systems project: the goal is to make distributed-systems
failure semantics explicit and verifiable, rather than to clone a production
cloud platform.

## Why it exists

A timeout or lost acknowledgement does not prove a job failed. A worker may have
already performed an external side effect before the coordinator retries it.
EpochFlow explores how a job engine can recover from those ambiguous failures
without letting stale workers overwrite newer work or blindly duplicating an
operation.

## Core guarantees

- **Durable job state:** job lifecycle information survives coordinator restarts.
- **Leased execution:** a worker owns a job only until its lease expires; it must
  renew while working.
- **Fencing:** every assignment has a monotonically increasing generation. A
  stale worker cannot commit a result after a newer assignment exists.
- **Idempotent operations:** retryable logical side effects use an operation ID
  so a retry returns the existing outcome instead of creating another one.
- **Explicit uncertainty:** an unconfirmed external request is not automatically
  treated as failed. It may be marked `UNKNOWN` until reconciliation is possible.

These guarantees do not provide magical exactly-once execution. Duplicate
*attempts* remain possible after failures; EpochFlow prevents or deduplicates the
effects it owns and makes unsupported external guarantees visible.

## Initial scope

Version 1 intentionally stays small:

- One C++20 coordinator and multiple independent C++20 worker processes.
- gRPC and Protobuf for worker/coordinator communication.
- PostgreSQL for durable job state, lease ownership, generations, and operations.
- Job states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, and `UNKNOWN`.
- Heartbeats, lease renewal, recovery of expired leases, fencing checks, and
  idempotency for a local demonstrator side effect.
- Fault-injection tests for crashes, pauses, duplicate delivery, coordinator
  restarts, and lost acknowledgements.

Out of scope for v1: multi-region consensus, autoscaling, Kubernetes scheduling,
custom database/storage engines, and unsupported exactly-once guarantees for
third-party APIs.

## Planned architecture

```text
Client
  │ submit job
  ▼
Coordinator ── durable state transitions ── PostgreSQL
  │ assign / renew / complete                 │
  └──────────── gRPC ─────────────── Workers ─┘
                                           │
                                           ▼
                                     task / side effect
```

The coordinator assigns work and owns recovery decisions. Workers execute jobs,
renew their leases independently of long-running task steps, and report results
with the assignment generation. PostgreSQL atomically accepts a transition only
when its expected state and generation still match.

## Roadmap

1. Build an in-process job runner with a clear state model and tests.
2. Split it into a coordinator and gRPC workers.
3. Add PostgreSQL-backed durable state, leases, and recovery.
4. Add fencing, idempotent operations, and fault injection.
5. Package a repeatable multi-worker demo with correctness and recovery metrics.

## Documentation

As implementation begins, detailed design and operational documentation lives in
[`docs/`](docs/), starting with architecture, invariants, storage, protocol,
testing, and ADRs. Contribution and agent working rules are in
[`AGENTS.md`](AGENTS.md).
