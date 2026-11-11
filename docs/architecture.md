# Architecture

## Purpose

Describes EpochFlow's components and their data and control flows. This page
tracks what exists; planned parts are marked as such.

## Context and terminology

- **Job:** a unit of work with a payload, a lifecycle state, and a result.
- **Coordinator:** assigns jobs, grants leases, and decides recovery.
- **Worker:** an independent process that acquires a job, renews its lease,
  executes the job, and reports a result.
- **Lease:** time-limited permission for one worker to execute one job.
- **Generation:** a per-job counter incremented on every assignment; it is the
  fencing token that rejects results from stale assignments.

## Design

Current status: **milestone 0 (bootstrap)**. The build, test harness, and test
database exist; no runtime components are implemented yet.

Planned components:

```text
Client ──submit──▶ Coordinator ──conditional updates──▶ PostgreSQL
                     ▲      │
        acquire /    │      │ recovery of expired leases
        renew /      │      ▼
        complete ────┴── Workers ──▶ task / side effect
               (gRPC)
```

- Workers **pull** work (`AcquireJob`); lease renewal doubles as the heartbeat.
- PostgreSQL is the single source of truth for job state, leases, and
  generations. Its clock (`now()`) decides lease expiry.

Planned job states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `UNKNOWN`.
The transition table is specified and tested in milestone 1.

## Invariants and failure semantics

See [invariants.md](invariants.md).

## Implementation map

| Path | Contents |
| --- | --- |
| `CMakeLists.txt`, `CMakePresets.json` | Build and dependency lookup |
| `tests/` | Unit and database tests |
| `docker-compose.test.yml`, `scripts/test-db.sh` | Test database |

## Trade-offs and limitations

- One coordinator; no coordinator replication in v1.
- PostgreSQL unavailability blocks all state transitions (by design in v1).

## Related documents

- [Invariants](invariants.md)
- [Testing](testing.md)
- [ADR 0001: toolchain and dependencies](decisions/0001-cpp-toolchain-and-dependencies.md)
