# Invariants

## Purpose

Lists the safety properties EpochFlow must preserve under failures and where
each one is enforced and tested. Each entry is updated when its enforcement code
lands.

## Context and terminology

See [architecture.md](architecture.md) for job, lease, generation, and reclaim.

## Invariants and failure semantics

| ID | Invariant | Status |
| --- | --- | --- |
| I1 | A job changes state only along an allowed transition. | In-memory (M1) and PostgreSQL (M3) stores |
| I2 | At most one assignment of a job is current at any time, and each assignment has a unique generation. | In-memory (M1) and PostgreSQL (M3) stores |
| I3 | A renewal, result, or side effect is accepted only from the job's current assignment: the job is `RUNNING` with the caller's generation. | Both stores (M1, M3), over gRPC (M2); side effects planned (M4) |
| I4 | One `operation_id` produces at most one logical side effect. | Planned (M4) |
| I5 | An unconfirmed external effect is recorded as `UNKNOWN`, never silently retried or marked failed. | Planned (M4) |
| I6 | Committed job state survives a coordinator restart. | PostgreSQL store (M3) |
| I7 | Once a job is terminal, its outcome (`result` or `error`) never changes. | Both stores (M1, M3) |

An expired lease permits recovery; it does not prove the previous worker
stopped. I3 is what makes recovery safe while that worker may still be running.
Lease timing affects liveness only: clock jumps can expire a lease early or
late, but cannot let two assignments both commit.

## Implementation map

| ID | Enforcement | Tests |
| --- | --- | --- |
| I1 | `IsAllowedTransition` (`src/core/job_state.cpp`); in memory, every change goes through `TransitionTo`, which throws `std::logic_error` on a violation; in PostgreSQL, each statement only performs one allowed transition (its `WHERE` names the source state) and `CHECK` constraints reject impossible rows (`sql/migrations/0001_jobs.sql`) | `JobState.*`, `SchemaRejectsRowsNoTransitionCanProduce` |
| I2 | In memory: claim and generation bump under the store mutex. In PostgreSQL: one `UPDATE` claims a `FOR UPDATE SKIP LOCKED` row and bumps the generation, re-checking `state = 'PENDING'` | `ConcurrentAcquiresAssignEachJobExactlyOnce`, `RunningJobIsNotAssignedAgain` (both stores) |
| I3 | In memory: `IsCurrentAssignment` check in `Renew`, `Complete`, `Fail`, in the same critical section as the update. In PostgreSQL: the same predicate in the `WHERE` clause of each `UPDATE` (`src/store/pg_job_store.cpp`). In both: `ReclaimExpired` moves expired jobs out of `RUNNING`; `Worker` never reports after a lost lease; over gRPC, stale maps to `FAILED_PRECONDITION` (`src/rpc/convert.cpp`) | `StaleWorkerCannotCompleteAfterReassignment`, `CompleteAfterReclaimBeforeReacquireIsStale`, `RenewAfterReclaimIsStale`, `LostLeaseCancelsTaskAndDiscardsItsResult`, `CoordinatorGrpcTest.StaleWorkerIsFencedAfterReassignment`, `CrashedWorkersJobIsRecoveredByAnotherWorker`, `e2e.smoke` |
| I6 | Every acknowledged change is committed before the RPC returns; the coordinator keeps no state of its own | `StateSurvivesStoreRestart`, `e2e.restart` |
| I7 | Duplicate check for the finishing generation returns `kAccepted`/`kConflict` without writing; this is what makes `Worker` report retries safe | `DuplicateCompleteWithDifferentResultConflicts`, `DuplicateFinalFailureIsIdempotent`, `CompleteAfterFinalFailureIsStale`, `RetriesReportWhenAcknowledgementIsLost` |

## Related documents

- [Architecture](architecture.md)
- [Testing](testing.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
- [Storage](storage.md)
