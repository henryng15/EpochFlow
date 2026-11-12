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

## Scope

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

## Architecture

```text
epochflowctl ── submit / get / reconcile ──┐
                                           ▼
                                  epochflow-coordinator ── one atomic, fenced
                                    (gRPC + reclaimer)      statement per change ──▶ PostgreSQL
                                           ▲                                        jobs, attempts,
      acquire / renew / effects / report   │                                        operations, ledger
                                  epochflow-worker × N ──▶ external provider
```

The coordinator assigns work and recovers expired leases. Workers renew their
leases on a separate thread, apply side effects through the coordinator, and
report results with their assignment generation. PostgreSQL accepts a change
only when the job is still `RUNNING` with that generation, and deduplicates
effects by operation id. Details: [architecture](docs/architecture.md).

## Status

Version 1 is complete: all five roadmap steps are done.

1. In-process job runner with a clear state model and tests.
2. Coordinator and gRPC workers.
3. PostgreSQL-backed durable state, leases, and recovery.
4. Fencing, idempotent operations, `UNKNOWN` outcomes, and fault injection.
5. Docker Compose demo and a chaos benchmark with correctness and recovery
   metrics.

Phase 2 is planned, not implemented. It prioritizes durable external-operation
confirmation, atomic operation-level reconciliation, and multi-seed benchmark
evidence before optional tracing or an AI example. See the
[Phase 2 plan](docs/phase-2.md).

## Results

`scripts/bench.py --mode compare --seed 7`: 200 payments, 3 workers, 1 s lease,
workers killed (SIGKILL) or frozen past their lease (SIGSTOP) every ~0.5 s.
Both runs replay the same chaos schedule; **naive** differs only in sending a
fresh operation id per attempt, like a client without idempotency keys.

| Metric | EpochFlow | Naive |
| --- | --- | --- |
| Jobs completed | 200 / 200 | 200 / 200 |
| Chaos events (kill / pause) | 11 (7 / 4) | 11 (7 / 4) |
| Retries | 10 | 10 |
| Throughput (jobs/s) | 29.0 | 29.2 |
| Recovery latency p50 / p95 (s) | 1.01 / 1.08 | 1.05 / 1.09 |
| Stale requests rejected by fencing | 1 | 1 |
| Effects deduplicated | 7 | 0 |
| **Duplicate ledger entries** | **0** | **5** |
| **Overcharged** | **$0.00** | **$5.00** |
| Invariant violations | 0 | 0 |

Recovery takes about one lease, as designed. The protections cost no measurable
throughput here; every crash between an effect and its report became a
duplicate charge without idempotency, and a deduplicated no-op with it. Without
fencing, the rejected stale request would also have overwritten a newer
result. Numbers vary from run to run; measured on WSL2 (16 vCPU) with
PostgreSQL 16 in Docker.

## Quick start

Requires Ubuntu 24.04 (or WSL2), Docker, and the apt packages listed in
[ADR 0001](docs/decisions/0001-cpp-toolchain-and-dependencies.md).

**Docker only** (no local build needed):

```bash
scripts/demo-docker.sh --down     # build, run, kill and freeze workers, check invariants
```

**Local build and tests:**

```bash
cmake --preset dev && cmake --build --preset dev
scripts/test-db.sh up
EPOCHFLOW_DSN="$(scripts/test-db.sh dsn)" ctest --preset dev     # all 200+ tests
scripts/bench.py --bin build/bin --dsn "$(scripts/test-db.sh dsn)" --mode compare
```

**Run it by hand:**

```bash
build/bin/epochflow-coordinator --dsn="$(scripts/test-db.sh dsn)" &
build/bin/epochflow-worker --worker-id=w1 --work-ms=200 &
build/bin/epochflow-worker --worker-id=w2 --work-ms=200 &
build/bin/epochflowctl wait $(build/bin/epochflowctl submit \
  --payload="task=payment;account=alice;amount_cents=500" --count=10)
```

See [operations](docs/operations.md) for flags, payloads, the `UNKNOWN`
runbook, and the benchmark.

## Documentation

- [Architecture](docs/architecture.md): components and data/control flows
- [Invariants](docs/invariants.md): safety properties and their enforcement
- [Protocol](docs/protocol.md): gRPC API semantics, errors, retry safety
- [Storage](docs/storage.md): PostgreSQL schema, transactions, migrations
- [Operations](docs/operations.md): running the binaries, configuration, logs
- [Testing](docs/testing.md): test layers, fault-injection scenarios, invariant checks
- [Phase 2 plan](docs/phase-2.md): proposed correctness fixes and evidence work
- [Decisions](docs/decisions/): ADRs

Contribution and agent working rules are in [`AGENTS.md`](AGENTS.md).
