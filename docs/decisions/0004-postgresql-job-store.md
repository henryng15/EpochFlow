# 0004. PostgreSQL job store: atomic statements, database time, SKIP LOCKED

## Status

Accepted (2026-11-12).

## Context

Milestone 3 makes job state durable (invariant I6). The durable store must keep
the fencing guarantees of ADR 0002 when several coordinator threads, or several
coordinator processes, issue requests concurrently, and it must agree on lease
expiry no matter which host asks.

## Decision

1. **One conditional statement per state change.** Fencing lives in the
   `WHERE` clause (`generation = $2 AND state = 'RUNNING'`), at the default
   `READ COMMITTED` isolation. No application-side read-then-write.
2. **Acquire uses `FOR UPDATE SKIP LOCKED`** on the oldest `PENDING` row, and
   the outer `UPDATE` re-checks `state = 'PENDING'`.
3. **The database clock is the only clock for leases.** Deadlines are
   `now() + lease` and expiry is `lease_until < now()`.
4. **Ambiguous storage outcomes propagate** as exceptions and become
   `UNAVAILABLE`; the store never guesses.
5. **Migrations are embedded in the binary** and applied at coordinator start
   under a schema-scoped advisory lock; the store refuses an outdated schema.
6. **A small fixed connection pool** with lazy connect and replacement of
   broken connections; no external pooler.
7. **Contract tests move time by shifting `lease_until`** instead of adding a
   test clock to production SQL, and each test uses its own schema.

## Consequences

- Correctness rests on PostgreSQL row locking, a well-understood mechanism,
  rather than on coordinator code; multiple coordinators are safe.
- `SERIALIZABLE` is not needed, so there are no serialization-failure retries.
- Coordinator clock skew cannot affect lease expiry; database clock jumps can
  (liveness only, as with any clock).
- Strict FIFO is approximate under concurrency: `SKIP LOCKED` may hand out a
  newer job while an older one is being claimed.
- Without the database, nothing progresses (accepted v1 limitation).
- The time-shifting harness works because lease expiry is the only time rule;
  a future time-based rule (for example retention) would need its own approach.

## Alternatives considered

- **`SERIALIZABLE` transactions with retry:** also correct, but adds retry
  loops for a problem single-statement updates already solve.
- **Advisory locks per job:** extra round trips and lock bookkeeping with no
  benefit over row locks.
- **Coordinator clock for deadlines:** breaks with multiple coordinators or
  clock jumps.
- **A test-only time offset parameter in every query:** leaks test concerns
  into production SQL.
- **External migration tool (Flyway, sqitch):** another dependency for one
  table; revisit if the schema grows or needs down migrations.

## References

- [Storage](../storage.md)
- [ADR 0002: job state model and fencing rules](0002-job-state-model-and-fencing-rules.md)
- PostgreSQL documentation: `SELECT ... FOR UPDATE SKIP LOCKED`, "Read Committed Isolation Level"
