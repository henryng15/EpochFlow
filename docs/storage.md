# Storage

## Purpose

How EpochFlow stores job state in PostgreSQL: schema, the SQL behind each
state change, transaction and concurrency behavior, migrations, and failure
handling.

## Context and terminology

- **`PgJobStore`:** the durable `JobStore` used by the coordinator with `--dsn`.
- **`InMemoryJobStore`:** the reference store for tests and the no-`--dsn` mode.
- **Fenced update:** an `UPDATE` whose `WHERE` clause includes
  `generation = $2 AND state = 'RUNNING'`, so the fencing check and the change
  are one atomic statement.

## Design

### Schema

`sql/migrations/0001_jobs.sql` creates one table:

| Column | Type | Meaning |
| --- | --- | --- |
| `job_id` | `BIGINT` identity | Job id, also submission order |
| `state` | `TEXT` | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `UNKNOWN` |
| `generation` | `BIGINT` | Fencing token; incremented only by acquire |
| `worker_id`, `lease_until` | `TEXT`, `TIMESTAMPTZ` | Lease holder and deadline; set exactly while `RUNNING` |
| `attempts`, `max_attempts` | `INTEGER` | Assignments so far and the limit |
| `payload`, `result` | `BYTEA` | Input; output once `COMPLETED` (never changed afterwards) |
| `error` | `TEXT` | Most recent failure or recovery reason |
| `created_at`, `updated_at` | `TIMESTAMPTZ` | Bookkeeping |

`CHECK` constraints reject rows no valid transition can produce: a lease
without `RUNNING` (or the reverse), a result without `COMPLETED`, attempts
above the limit, a non-`PENDING` job with generation 0, unknown states. Partial
indexes serve the two hot queries: `PENDING` by id (acquire) and `RUNNING` by
deadline (reclaim).

### State changes

Every `PgJobStore` method is one transaction at the default `READ COMMITTED`
isolation:

| Operation | SQL shape |
| --- | --- |
| Submit | `INSERT ... RETURNING job_id` |
| Acquire | CTE `SELECT job_id ... WHERE state='PENDING' ORDER BY job_id LIMIT 1 FOR UPDATE SKIP LOCKED`, then `UPDATE ... SET state='RUNNING', generation=generation+1, attempts=attempts+1, lease_until=now()+lease ... AND state='PENDING' RETURNING ...` |
| Renew | Fenced `UPDATE ... SET lease_until = now() + lease` |
| Complete | Fenced `UPDATE ... SET state='COMPLETED', result=$3`, clear lease |
| Fail | Fenced `UPDATE ... SET state = CASE WHEN retryable AND attempts < max_attempts THEN 'PENDING' ELSE 'FAILED' END` |
| Reclaim | `UPDATE ... WHERE state='RUNNING' AND lease_until < now()`: `PENDING` or `FAILED` by attempts |

When a fenced update matches no row, a follow-up `SELECT` in the same
transaction classifies the request: no row is `kNotFound`; the job finished by
this generation with the same outcome is `kAccepted` (idempotent duplicate),
with a different outcome `kConflict`; anything else is `kStale`. A finished job
never changes, so this read cannot race with a change that alters the answer.

### Time

Lease deadlines are written and compared with the database's `now()`. The
coordinator's clock is never used, so several coordinators, or a coordinator
whose clock jumps, still agree on expiry. Timestamps leave the database as
integer microseconds since the Unix epoch.

### Connections

`PgConnectionPool` holds up to `--db-pool-size` connections (default 4),
opened lazily and leased exclusively per transaction (`pqxx::connection` is not
thread-safe). A connection that is no longer open when returned is dropped and
replaced on a later acquire, so the store recovers from a database restart or
terminated backend without a coordinator restart.

### Migrations

Migration files are embedded into the binary at configure time.
`MigrateDatabase` runs at coordinator startup: inside one transaction it takes
`pg_advisory_xact_lock(class, hashtext(current_schema()))`, creates
`schema_migrations` if needed, and applies every missing migration in version
order. `PgJobStore` refuses to start unless the schema is at the latest
version. The schema used is the connection's `search_path` (for example
`options='-csearch_path=myschema'` in the DSN).

## Invariants and failure semantics

- **I1-I3:** the transition, the generation bump, and the fencing check are all
  in one statement; PostgreSQL row locks serialize competing statements on the
  same job, and `READ COMMITTED` re-evaluates the `WHERE` clause after waiting,
  so a stale request can never apply after a newer one.
- **Claim liveness:** `SKIP LOCKED` makes concurrent acquirers take different
  rows; without it they queue on the same row and the losers return empty
  while jobs remain (caught by `ConcurrentAcquiresFindAJobWhileAnyIsUnclaimed`).
- **I6:** every acknowledged change is committed before the RPC returns.
- **Ambiguous outcomes:** a connection lost mid-transaction or during commit
  (`pqxx::broken_connection`, `pqxx::in_doubt_error`) propagates; the
  coordinator answers `UNAVAILABLE` and the worker may retry, which is safe for
  renew, complete, and fail.
- **Database unavailable:** every RPC fails with `UNAVAILABLE`; no state
  changes happen, workers lose their leases after failed renewals, and work
  resumes when the database returns. Safety is preserved; progress stops.

## Implementation map

| Path | Contents |
| --- | --- |
| `sql/migrations/0001_jobs.sql` | Schema and constraints |
| `src/store/migrations.*`, `src/store/embedded_migrations.cpp.in` | Migration runner and embedding |
| `src/store/pg_connection_pool.*` | Connection pool |
| `src/store/pg_job_store.*` | SQL for every operation |
| `tests/integration/pg_job_store_test.cpp` | Contract on PostgreSQL plus storage tests |
| `tests/e2e/restart_test.sh` | Coordinator restart with real processes |

## Operating and testing

See [operations.md](operations.md) for `--dsn` and [testing.md](testing.md) for
how the contract suite moves database time.

## Trade-offs and limitations

- **Single table, no attempt history.** Recovery traces (which worker ran which
  generation) come from logs for now; an attempts table arrives with fault
  injection in milestone 4, where traces are needed.
- **Polling reclaim:** one `UPDATE` per reclaim interval per coordinator. Running
  several coordinators is safe but each runs its own reclaim.
- **No job retention policy:** finished jobs stay forever.
- **Transitions are enforced in C++ SQL, not by a trigger.** `CHECK`
  constraints catch impossible rows but not every illegal transition (for
  example `FAILED` back to `PENDING`).
- Migrations are forward-only; there are no down migrations.

## Related documents

- [Architecture](architecture.md)
- [Invariants](invariants.md)
- [Operations](operations.md)
- [ADR 0004: PostgreSQL job store](decisions/0004-postgresql-job-store.md)
