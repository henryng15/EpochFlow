# Testing

## Purpose

How EpochFlow is tested, how to run each test layer, and which failure cases
each layer must cover.

## Context and terminology

- **Unit tests** (CTest label `unit`): in-process, no external services.
- **Database tests** (CTest label `db`): need PostgreSQL through the
  `EPOCHFLOW_DSN` environment variable. They call `GTEST_SKIP()` when it is
  unset, so a plain local run never needs Docker.
- **Fault-injection tests** (planned, milestone 4): start real coordinator and
  worker processes, then crash, pause, or duplicate requests at chosen points.

## Design

Tests are written before the behavior they specify (see `AGENTS.md`). Every
distributed behavior needs its safety or recovery case, not only the happy path:
lease expiry, stale generation, duplicate request, crash, or timeout.

Lease behavior is tested with an injected clock in unit tests and with the
database clock in `db` tests; tests must not rely on wall-clock sleeps to make
a lease expire.

## Operating and testing

Prerequisites: the apt packages listed in
[ADR 0001](decisions/0001-cpp-toolchain-and-dependencies.md) and Docker.

```bash
cmake --preset dev                 # configure into build/
cmake --build --preset dev         # build, 2 parallel jobs
ctest --preset unit                # unit tests only

scripts/test-db.sh up              # start disposable PostgreSQL on :15432
EPOCHFLOW_DSN="$(scripts/test-db.sh dsn)" ctest --preset dev   # all tests
scripts/test-db.sh down            # stop and discard data
```

CI (`.github/workflows/ci.yml`) runs all tests against a PostgreSQL service
container and **fails if any test is skipped**, so a broken database setup
cannot pass silently.

## Implementation map

| Path | Contents |
| --- | --- |
| `tests/unit/` | Unit tests, binary `epochflow_unit_tests` |
| `tests/integration/` | Database tests, binary `epochflow_integration_tests` |
| `docker-compose.test.yml`, `scripts/test-db.sh` | Disposable test database (tmpfs data) |

## Trade-offs and limitations

- Database tests share one database. Tests that write data must isolate
  themselves (unique IDs or per-test schema) once storage lands in milestone 3.
- Process-level fault injection does not exist yet.

## Related documents

- [Architecture](architecture.md)
- [Invariants](invariants.md)
- [ADR 0001: toolchain and dependencies](decisions/0001-cpp-toolchain-and-dependencies.md)
