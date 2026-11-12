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

Four migrations: `0001_jobs.sql` (jobs), `0002_operations.sql` (operations
and ledger), `0003_job_attempts.sql` (attempt trace),
`0004_operation_reconciliation.sql` (operator-resolved external operations).

**`jobs`**

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

**`operations`**: one row per logical side effect, keyed by a caller-chosen
`operation_id` that stays the same across attempts and duplicate jobs.

| Column | Meaning |
| --- | --- |
| `operation_id` | Primary key, non-empty |
| `kind` | `LEDGER` (owned effect) or `EXTERNAL` (third-party call) |
| `request` | Canonical content; reuse with different content is a conflict |
| `status` | `APPLIED` (ledger), `STARTED` (external intent), `CONFIRMED` (external answer recorded), `FAILED` (an operator proved the external effect never happened; terminal) |
| `job_id`, `generation` | The assignment that created the row |
| `result` | Provider confirmation (`CONFIRMED`) or the operator's evidence (`FAILED`); present exactly in those two states |
| `reconciled_at` | When an operator decision resolved the row; required for `FAILED`, NULL when a worker confirmed it |

**`ledger_entries`**: the owned demonstration effect; `UNIQUE (operation_id)`
makes a second entry for one operation impossible. Records the job and
generation that applied it.

**`job_attempts`**: one row per assignment (`PRIMARY KEY (job_id,
generation)`), with `worker_id`, start and finish times, and `outcome`:
`COMPLETED`, `FAILED`, `REQUEUED`, `LEASE_EXPIRED`, `UNKNOWN`, or `NULL` while
the attempt holds the job. Not backfilled for jobs assigned before migration 3.

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
| MarkUnknown | Fenced `UPDATE ... SET state='UNKNOWN', error=$3`, clear lease |
| Reconcile | `UPDATE ... WHERE job_id=$1 AND state='UNKNOWN' AND NOT EXISTS (a STARTED external operation of this job)` to `COMPLETED` (result) or `FAILED` (error) |
| ReconcileExternal | One transaction: `SELECT ... FOR UPDATE` on the job, then on the operation (the only path holding both, so no lock-order cycle); check ownership and states; `UPDATE operations` to `CONFIRMED`/`FAILED` with the evidence and `reconciled_at`; `UPDATE jobs` to `COMPLETED`/`FAILED`. Any error rolls both back |
| ApplyLedger | Fenced `SELECT ... FOR SHARE` on the job; `INSERT INTO operations ... ON CONFLICT DO NOTHING`; if inserted, `INSERT INTO ledger_entries`; otherwise read the existing operation and entry |
| BeginExternal | Fenced `SELECT ... FOR SHARE`; `INSERT ... 'STARTED' ON CONFLICT DO NOTHING`; otherwise read status, creator, and confirmation |
| ConfirmExternal | `UPDATE operations SET status='CONFIRMED', result=$3 WHERE ... status='STARTED'` (not fenced); a `FAILED` row is a conflict |

Every job change that starts or ends an assignment also writes `job_attempts`
in the same transaction: acquire inserts the row; complete, fail, mark-unknown,
and reclaim close it with the matching outcome (reclaim through a
data-modifying CTE).

**Fenced effects.** `ApplyLedger` and `BeginExternal` first lock the job row
with `FOR SHARE` under the fencing predicate. Reclaim, reassignment, and
completion need a row-exclusive lock, so they wait until the effect's
transaction ends: the check and the effect are atomic, and an effect can never
be recorded for an assignment that was superseded mid-transaction. Concurrent
inserts of the same `operation_id` serialize on the primary key; the loser's
`ON CONFLICT DO NOTHING` waits for the winner and then reads its row.

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
- **I4:** one ledger entry per operation id, by `UNIQUE` plus the transaction
  above; external calls are guarded by the intent record (see
  [ADR 0005](decisions/0005-idempotent-operations-and-unknown-outcomes.md)).
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
| `sql/migrations/` | Schema and constraints (jobs, operations, ledger, attempts) |
| `scripts/check-invariants.sql` | Read-only invariant checker |
| `src/store/migrations.*`, `src/store/embedded_migrations.cpp.in` | Migration runner and embedding |
| `src/store/pg_connection_pool.*` | Connection pool |
| `src/store/pg_job_store.*` | SQL for every operation |
| `tests/integration/pg_job_store_test.cpp` | Contract on PostgreSQL plus storage tests |
| `tests/e2e/restart_test.sh` | Coordinator restart with real processes |

## Operating and testing

See [operations.md](operations.md) for `--dsn` and the invariant checker, and
[testing.md](testing.md) for how the contract suite moves database time.

## Trade-offs and limitations

- **No retention policy** for jobs, operations, ledger entries, or attempts.
- **Polling reclaim:** one `UPDATE` per reclaim interval per coordinator. Running
  several coordinators is safe but each runs its own reclaim.
- **Transitions are enforced in C++ SQL, not by a trigger.** `CHECK`
  constraints catch impossible rows but not every illegal transition (for
  example `FAILED` back to `PENDING`).
- Migrations are forward-only; there are no down migrations.

## Related documents

- [Architecture](architecture.md)
- [Invariants](invariants.md)
- [Operations](operations.md)
- [ADR 0004: PostgreSQL job store](decisions/0004-postgresql-job-store.md)
- [ADR 0005: idempotent operations and UNKNOWN outcomes](decisions/0005-idempotent-operations-and-unknown-outcomes.md)
- [ADR 0006: operation-level reconciliation](decisions/0006-operation-level-reconciliation.md)
