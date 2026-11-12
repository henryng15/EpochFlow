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

1. Build an in-process job runner with a clear state model and tests. **(done)**
2. Split it into a coordinator and gRPC workers. **(done)**
3. Add PostgreSQL-backed durable state, leases, and recovery. **(done)**
4. Add fencing, idempotent operations, and fault injection.
5. Package a repeatable multi-worker demo with correctness and recovery metrics.

## Quick start

Requires Ubuntu 24.04 (or WSL2), Docker, and the apt packages listed in
[ADR 0001](docs/decisions/0001-cpp-toolchain-and-dependencies.md).

```bash
cmake --preset dev && cmake --build --preset dev
ctest --preset unit

scripts/test-db.sh up
EPOCHFLOW_DSN="$(scripts/test-db.sh dsn)" ctest --preset dev
```

Run a coordinator on PostgreSQL, two workers, and ten jobs:

```bash
scripts/test-db.sh up
build/bin/epochflow-coordinator --dsn="$(scripts/test-db.sh dsn)" &
build/bin/epochflow-worker --worker-id=w1 --work-ms=200 &
build/bin/epochflow-worker --worker-id=w2 --work-ms=200 &
build/bin/epochflowctl wait $(build/bin/epochflowctl submit --payload=hello --count=10)
```

See [operations](docs/operations.md) for flags and a crash-recovery walkthrough.

## Documentation

- [Architecture](docs/architecture.md): components and data/control flows
- [Invariants](docs/invariants.md): safety properties and their enforcement
- [Protocol](docs/protocol.md): gRPC API semantics, errors, retry safety
- [Storage](docs/storage.md): PostgreSQL schema, transactions, migrations
- [Operations](docs/operations.md): running the binaries, configuration, logs
- [Testing](docs/testing.md): test layers and how to run them
- [Decisions](docs/decisions/): ADRs

Contribution and agent working rules are in [`AGENTS.md`](AGENTS.md).
