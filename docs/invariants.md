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
| I1 | A job changes state only along an allowed transition. | In-memory store (M1); PostgreSQL planned (M3) |
| I2 | At most one assignment of a job is current at any time, and each assignment has a unique generation. | In-memory store (M1); PostgreSQL planned (M3) |
| I3 | A renewal, result, or side effect is accepted only from the job's current assignment: the job is `RUNNING` with the caller's generation. | In-memory store (M1), over gRPC (M2); side effects planned (M4) |
| I4 | One `operation_id` produces at most one logical side effect. | Planned (M4) |
| I5 | An unconfirmed external effect is recorded as `UNKNOWN`, never silently retried or marked failed. | Planned (M4) |
| I6 | Committed job state survives a coordinator restart. | Planned (M3) |
| I7 | Once a job is terminal, its outcome (`result` or `error`) never changes. | In-memory store (M1) |

An expired lease permits recovery; it does not prove the previous worker
stopped. I3 is what makes recovery safe while that worker may still be running.
Lease timing affects liveness only: clock jumps can expire a lease early or
late, but cannot let two assignments both commit.

## Implementation map

| ID | Enforcement | Tests |
| --- | --- | --- |
| I1 | `IsAllowedTransition` (`src/core/job_state.cpp`); every store change goes through `TransitionTo`, which throws `std::logic_error` on a violation (`src/store/in_memory_job_store.cpp`) | `JobState.*` |
| I2 | `Acquire` claims a `PENDING` job and increments its generation under the store mutex | `ConcurrentAcquiresAssignEachJobExactlyOnce`, `RunningJobIsNotAssignedAgain` |
| I3 | `IsCurrentAssignment` check in `Renew`, `Complete`, `Fail`, in the same critical section as the update; `ReclaimExpired` moves expired jobs out of `RUNNING`; `Worker` never reports after a lost lease; over gRPC, stale maps to `FAILED_PRECONDITION` (`src/rpc/convert.cpp`) | `StaleWorkerCannotCompleteAfterReassignment`, `CompleteAfterReclaimBeforeReacquireIsStale`, `RenewAfterReclaimIsStale`, `LostLeaseCancelsTaskAndDiscardsItsResult`, `CoordinatorGrpcTest.StaleWorkerIsFencedAfterReassignment`, `CrashedWorkersJobIsRecoveredByAnotherWorker`, `e2e.smoke` |
| I7 | Duplicate check for the finishing generation returns `kAccepted`/`kConflict` without writing; this is what makes `Worker` report retries safe | `DuplicateCompleteWithDifferentResultConflicts`, `DuplicateFinalFailureIsIdempotent`, `CompleteAfterFinalFailureIsStale`, `RetriesReportWhenAcknowledgementIsLost` |

## Related documents

- [Architecture](architecture.md)
- [Testing](testing.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
