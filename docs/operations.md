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
| worker | `--work-ms` | 0 | Simulated processing time per job, before its side effect |
| worker | `--provider-log` | none | File acting as the external provider; required for `task=external` jobs |
| worker | `--fault` | none | Test only: inject one failure (see `epochflow-worker --help`) |
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

### Job payloads

| Payload | Effect |
| --- | --- |
| `task=payment;account=A;amount_cents=N[;op=ID]` | One ledger entry per `op` (default `job-<id>`) |
| `task=external;account=A;amount_cents=N[;op=ID]` | At most one provider charge per `op` |
| anything else | Echo: result `processed:<payload>` |

Use a stable `op` for anything a client might submit twice (for example an
order number): every job with that `op` and content shares one effect.

### Resolving UNKNOWN jobs

A job is `UNKNOWN` when the worker could not tell whether the external charge
happened. It stays that way until resolved:

1. Find it: `epochflowctl get JOB_ID` shows `state=UNKNOWN` and the reason in
   `error`; `SELECT job_id, error FROM jobs WHERE state = 'UNKNOWN'` lists all.
2. Check the provider's own record for the operation id (with the file
   provider: `awk -F'\t' '$1 == "OPERATION_ID"' PROVIDER_LOG`; the third
   column is the confirmation).
3. Record the decision:
   - charged: `epochflowctl reconcile JOB_ID --outcome=completed --detail=CONFIRMATION`
   - not charged: `epochflowctl reconcile JOB_ID --outcome=failed --detail="no charge at provider"`

Do not resubmit the job to "try again" without checking: that is exactly the
double charge `UNKNOWN` exists to prevent.

### Checking invariants

```bash
psql "$DSN" -X -q -tA -F '|' -f scripts/check-invariants.sql
```

No output means every safety check passed. It is a single read-only
statement, so it sees one consistent snapshot and is safe on a live system. A
`recovery stalled` row means a `RUNNING` job's lease expired over a minute ago
and no coordinator is reclaiming.

### Logs

Each process logs one line per event to stderr, prefixed with a UTC timestamp.
Coordinator: `coordinator started store=postgres|memory`, `assigned`,
`reclaimed`, `rejected completion`, `deduplicated operation`, `rejected
operation`, `in doubt operation`, `unknown job`, `reconciled job`, store errors.
Worker: `acquired`, `reported success|failure|unknown ... status=`, `lease
lost`, `report attempt failed`, `gave up reporting`, `fault=... firing`.

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
- **A job is `UNKNOWN`**: see "Resolving UNKNOWN jobs" above.
- **A payment job `FAILED` with `conflicts with an earlier request`**: its `op`
  was already used with different content; choose a new `op` if it is really a
  different payment.
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
