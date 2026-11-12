# Testing

## Purpose

How EpochFlow is tested, how to run each test layer, and which failure cases
each layer must cover.

## Context and terminology

- **Unit tests** (CTest label `unit`): in-process, no external services.
- **Database tests** (CTest label `db`): need PostgreSQL through the
  `EPOCHFLOW_DSN` environment variable. They call `GTEST_SKIP()` when it is
  unset, so a plain local run never needs Docker.
- **gRPC tests** (CTest label `grpc`): coordinator, client, and workers over a
  loopback connection in one process. No external services.
- **End-to-end tests** (CTest label `e2e`): real `epochflow-coordinator`,
  `epochflow-worker`, and `epochflowctl` processes driven by a shell script.
- **Fault-injection tests** (planned, milestone 4): crash, pause, or duplicate
  requests at chosen points with an invariant checker.

## Design

Tests are written before the behavior they specify (see `AGENTS.md`). Every
distributed behavior needs its safety or recovery case, not only the happy path:
lease expiry, stale generation, duplicate request, crash, or timeout.

Lease behavior is tested with an injected clock in unit tests and with the
database clock in `db` tests; tests must not rely on wall-clock sleeps to make
a lease expire.

### JobStore contract suite

`tests/contract/` holds the behavioral specification of `JobStore` as a
parameterized GoogleTest suite (`JobStoreContractTest`). Each store provides a
`StoreHarness` (a fresh store plus `AdvanceTime`) and instantiates the suite;
the in-memory store does so in `tests/unit/in_memory_job_store_test.cpp` and
the PostgreSQL store in `tests/integration/pg_job_store_test.cpp`. Contract
tests observe only the interface and relative time, never absolute timestamps.
Checks that depend on an exact clock belong next to the specific store.

A factory may return `nullptr` to skip (the PostgreSQL one does without
`EPOCHFLOW_DSN`).

**PostgreSQL time:** the store uses the database clock, so the PostgreSQL
harness implements `AdvanceTime(d)` by moving every `lease_until` back by `d`.
Lease expiry is the only time-dependent rule, so the store observes exactly
what a clock jump would produce, and production SQL needs no test hook.

**Isolation:** every PostgreSQL test runs in its own schema
(`tests/support/pg_test_schema.h`) selected through the DSN's `search_path`,
with a unique `application_name`, so parallel CTest processes never share rows
and a test can terminate exactly its own connections.

The suite covers: acquire ordering, exclusivity, and liveness (8 threads racing
for 200 jobs; no acquire may come back empty while a job is unclaimed), stale generation on renew/complete/fail, reassignment after
expiry, the window between deadline and reclaim, duplicate and conflicting
completions, retry limits, and reclaim selectivity.

### gRPC and processes

`tests/grpc/coordinator_grpc_test.cpp` keeps a `ManualClock` behind the
coordinator, so lease expiry over the wire is still test-driven; it checks every
`CommitStatus` to status-code mapping and transport-error behavior.
`tests/grpc/distributed_test.cpp` uses the system clock with a 300 ms lease to
run workers against the coordinator and recover a "crashed" assignment (one
that acquires and never renews).

`tests/e2e/restart_test.sh` (labels `e2e` and `db`) kills the coordinator and
the worker holding a lease with SIGKILL, restarts the coordinator on the same
PostgreSQL schema, checks that every job kept its state and generation, and
expects the interrupted job to finish on attempt 2. It exits 77 (skipped)
without `EPOCHFLOW_DSN` or `psql`.

`tests/e2e/smoke_test.sh` starts the coordinator on a free port, runs a batch
through two workers, stops them with SIGTERM (must exit 0), then kills a slow
worker with SIGKILL mid-job and expects another worker to finish the job with
`attempts=2` and `generation=2`.

### Threaded components

`LeaseKeeper` and `LocalRunner` tests run real threads with short intervals and
long timeouts (`tests/support/wait.h`), asserting outcomes, never timing. Lost
leases are simulated with a store decorator that rejects renewals, not with
sleeps.

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

ThreadSanitizer build of the unit tests (separate `build-tsan/` directory):

```bash
cmake --preset tsan && cmake --build --preset tsan
setarch -R ctest --preset tsan     # -R disables ASLR, see Troubleshooting
```

Run it after changing anything that shares state between threads.

CI (`.github/workflows/ci.yml`) runs all tests against a PostgreSQL service
container and **fails if any test is skipped**, so a broken database setup
cannot pass silently.

## Implementation map

| Path | Contents |
| --- | --- |
| `tests/unit/` | Unit tests, binary `epochflow_unit_tests` |
| `tests/contract/` | `JobStore` contract suite and `StoreHarness` |
| `tests/support/` | `ManualClock`, `WaitUntil`, `PgTestSchema` |
| `tests/grpc/` | gRPC tests, binary `epochflow_grpc_tests` |
| `tests/e2e/` | Process-level tests (`e2e.smoke`, `e2e.restart`) |
| `tests/integration/` | Database tests (contract on PostgreSQL, storage), binary `epochflow_integration_tests` |
| `docker-compose.test.yml`, `scripts/test-db.sh` | Disposable test database (tmpfs data) |

## Troubleshooting

- `FATAL: ThreadSanitizer: unexpected memory mapping`: recent kernels randomize
  memory layout beyond what GCC 13's TSan supports. Run under `setarch -R`, as
  above. CTest discovers tests at run time (`DISCOVERY_MODE PRE_TEST`) so the
  wrapper also covers discovery.

## Trade-offs and limitations

- Database tests need Docker locally; CI provides a service container.
- The e2e test covers one crash point (during work). Crashes at other points,
  pauses, and lost acknowledgements between processes arrive with fault
  injection in milestone 4.
- The ThreadSanitizer preset builds unit tests only; gRPC's own libraries are
  not instrumented, which would produce false reports.

## Related documents

- [Architecture](architecture.md)
- [Invariants](invariants.md)
- [ADR 0001: toolchain and dependencies](decisions/0001-cpp-toolchain-and-dependencies.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
- [Protocol](protocol.md)
