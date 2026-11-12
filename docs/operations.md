# Operations

## Purpose

How to run the EpochFlow binaries locally, what their flags mean, and how to
tell whether recovery is working.

## Prerequisites

A build (`cmake --preset dev && cmake --build --preset dev`). Binaries are in
`build/bin/`.

## Steps

Start PostgreSQL (the disposable test database works for local use), a
coordinator, two workers, and submit jobs (separate terminals, or background
them):

```bash
scripts/test-db.sh up
build/bin/epochflow-coordinator --lease-ms=3000 --dsn="$(scripts/test-db.sh dsn)"
build/bin/epochflow-worker --worker-id=w1 --work-ms=500
build/bin/epochflow-worker --worker-id=w2 --work-ms=500

ids=$(build/bin/epochflowctl submit --payload=hello --count=10)
build/bin/epochflowctl wait $ids
build/bin/epochflowctl get 1
```

The schema is the DSN's `search_path` (default `public`); to keep several
deployments in one database, add `options='-csearch_path=NAME'` and create the
schema first. Migrations run at every start and are no-ops when current.

To watch recovery, start a slow worker (`--work-ms=30000`), let it acquire a
job, `kill -9` it, and start another worker: within one lease plus one reclaim
interval the coordinator logs `reclaimed 1 expired lease(s)` and the job runs
again with `generation=2`.

### Configuration

| Binary | Flag | Default | Meaning |
| --- | --- | --- | --- |
| coordinator | `--listen` | `127.0.0.1:50051` | Serve address; port 0 picks a free port |
| coordinator | `--lease-ms` | 10000 | Lease granted per acquire/renewal |
| coordinator | `--reclaim-interval-ms` | 1000 | Expired-lease recovery period |
| coordinator | `--dsn` | none | PostgreSQL connection string; migrates at startup. Without it, state is in memory only |
| coordinator | `--db-pool-size` | 4 | PostgreSQL connections |
| worker | `--coordinator` | `127.0.0.1:50051` | Coordinator address |
| worker | `--worker-id` | `HOST-PID` | Identity in job records and logs |
| worker | `--work-ms` | 0 | Duration of the built-in demo task |
| worker | `--rpc-timeout-ms` | 5000 | Deadline per RPC |
| worker | `--idle-backoff-ms` | 200 | Wait when no job is available |
| epochflowctl | `--coordinator` | `127.0.0.1:50051` | Coordinator address |

All binaries take `--help`. Exit status: 0 success, 1 runtime failure, 2 usage
error.

**Choosing timings:** a crashed worker's job restarts after at most
`lease + reclaim-interval`. A worker waits `lease / 3` between renewals and a
renewal can take up to `rpc-timeout`, so it lands in time only if
`lease / 3 + rpc-timeout < lease`, that is `--rpc-timeout-ms` below two thirds
of `--lease-ms` (the defaults, 5000 and 10000, satisfy this). Leave headroom
for slow networks.

### Logs

Each process logs one line per event to stderr, prefixed with a UTC timestamp.
Coordinator: `coordinator started store=postgres|memory`, `assigned`,
`reclaimed`, `rejected completion`, store errors.
Worker: `acquired`, `reported ... status=`, `lease lost`, `report attempt
failed`, `gave up reporting`.

## Verification

```bash
ctest --preset dev -L e2e --output-on-failure
```

runs the same scenario automatically, including the SIGKILL recovery.

## Troubleshooting

- **`cannot listen on ...`**: the port is taken; choose another or use port 0.
- **Worker logs `acquire failed: rpc failed (14)`**: coordinator unreachable;
  the worker keeps retrying every idle backoff.
- **Jobs re-run though workers are healthy (`lease lost`)**: renewals are not
  arriving in time; raise `--lease-ms` or lower `--rpc-timeout-ms`.
- **All jobs vanished after a restart**: the coordinator ran without `--dsn`
  (its log says `store=memory`).
- **`database schema is at version N, expected M`**: a newer binary needs a
  migration that failed or an older binary met a newer schema; check the
  coordinator log for the migration error.
- **RPCs fail with `rpc failed (14)` and the coordinator logs `store error`**:
  PostgreSQL is unreachable. No state changes until it returns; the pool
  reconnects automatically.
- **`ports are not available` from `scripts/test-db.sh up` on WSL**: Windows
  reserves port ranges above 49152 (see ADR 0001); the test database uses 15432.

## Related documents

- [Protocol](protocol.md)
- [Storage](storage.md)
- [Architecture](architecture.md)
- [Testing](testing.md)
