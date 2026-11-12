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

### JobStore contract suite

`tests/contract/` holds the behavioral specification of `JobStore` as a
parameterized GoogleTest suite (`JobStoreContractTest`). Each store provides a
`StoreHarness` (a fresh store plus `AdvanceTime`) and instantiates the suite;
the in-memory store does so in `tests/unit/in_memory_job_store_test.cpp`, and
the PostgreSQL store will reuse it in milestone 3. Contract tests observe only
the interface and relative time, never absolute timestamps. Checks that depend
on an exact clock belong next to the specific store.

The suite covers: acquire ordering and exclusivity (including 8 threads racing
for 200 jobs), stale generation on renew/complete/fail, reassignment after
expiry, the window between deadline and reclaim, duplicate and conflicting
completions, retry limits, and reclaim selectivity.

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
| `tests/support/` | `ManualClock`, `WaitUntil` |
| `tests/integration/` | Database tests, binary `epochflow_integration_tests` |
| `docker-compose.test.yml`, `scripts/test-db.sh` | Disposable test database (tmpfs data) |

## Troubleshooting

- `FATAL: ThreadSanitizer: unexpected memory mapping`: recent kernels randomize
  memory layout beyond what GCC 13's TSan supports. Run under `setarch -R`, as
  above. CTest discovers tests at run time (`DISCOVERY_MODE PRE_TEST`) so the
  wrapper also covers discovery.

## Trade-offs and limitations

- Database tests share one database. Tests that write data must isolate
  themselves (unique IDs or per-test schema) once storage lands in milestone 3.
- Process-level fault injection does not exist yet.

## Related documents

- [Architecture](architecture.md)
- [Invariants](invariants.md)
- [ADR 0001: toolchain and dependencies](decisions/0001-cpp-toolchain-and-dependencies.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
