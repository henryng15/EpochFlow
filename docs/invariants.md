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
| I3 | A renewal, result, or side effect is accepted only from the job's current assignment: the job is `RUNNING` with the caller's generation. | Both stores, over gRPC, and for side effects (M1-M4) |
| I4 | One `operation_id` produces at most one logical side effect: exactly one ledger entry, at most one external call whose outcome is known to EpochFlow. | Both stores (M4) |
| I5 | An unconfirmed external effect is recorded as `UNKNOWN`, never silently retried or marked failed. | Both stores and workers (M4) |
| I6 | Committed job state survives a coordinator restart. | PostgreSQL store (M3) |
| I7 | Once a job is terminal, its outcome (`result` or `error`) never changes. | Both stores (M1, M3) |
| I8 | A worker reports a provider effect as completed only after the durable operation record confirms that operation, request, and provider answer. | Workers (Phase 2, P0) |

External effects are at most once, not exactly once: a provider without
idempotency keys cannot be deduplicated, so EpochFlow guarantees only that it
never repeats a call whose outcome it does not know (see
[ADR 0005](decisions/0005-idempotent-operations-and-unknown-outcomes.md)).

**Checking from data.** `scripts/check-invariants.sql` verifies I2-I4 from the
tables at rest (attempt numbering, one open attempt, one deciding attempt from
the current generation, state matching the deciding attempt, one ledger entry
per operation, effects applied by real assignments), I8 (no worker-completed
job owns an external operation that is still `STARTED`), plus stalled recovery.
Every PostgreSQL contract test and every fault-injection scenario ends by
running it.

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
| I4 | Ledger: fenced share lock, operation insert, and entry insert in one transaction; `UNIQUE (operation_id)` on `ledger_entries`; in memory, the same steps under the store mutex. External: intent row (`STARTED`) before the provider call; another attempt's unconfirmed intent yields `kInDoubt` | `ApplyLedger*` and `ConcurrentApplyOfOneOperationCreatesOneEntry` (both stores), `test_crash_after_effect_is_deduplicated`, `test_duplicate_requests_create_one_effect`, `test_coordinator_crash_mid_run_loses_and_duplicates_nothing`, checker `I4 ledger operation has entry` |
| I5 | `ExternalState::kInDoubt` and provider timeouts become `JobUnknown` in the external task (`src/worker/tasks.cpp`); `MarkUnknown` parks the job outside reclaim and acquire; only `Reconcile` leaves `UNKNOWN` | `MarkUnknownParksJobOutsideRetryAndRecovery`, `UnconfirmedIntentFromEarlierAttemptIsInDoubt`, `test_external_timeout_is_unknown_until_reconciled`, `test_crash_during_external_call_is_never_charged_twice` |
| I6 | Every acknowledged change is committed before the RPC returns; the coordinator keeps no state of its own | `StateSurvivesStoreRestart`, `e2e.restart` |
| I7 | Duplicate check for the finishing generation returns `kAccepted`/`kConflict` without writing; this is what makes `Worker` report retries safe | `DuplicateCompleteWithDifferentResultConflicts`, `DuplicateFinalFailureIsIdempotent`, `CompleteAfterFinalFailureIsStale`, `RetriesReportWhenAcknowledgementIsLost` |
| I8 | After the provider call, the external task (`src/worker/tasks.cpp`) succeeds only if `ConfirmExternal` returns `kAccepted` (an identical duplicate counts, so a lost acknowledgement is safe); a rejection, or three transport failures, returns `JobUnknown` without calling the provider again | `RejectedConfirmationMakesTheOutcomeUnknownWithoutCallingTheProviderAgain`, `ConfirmationThatNeverReachesTheStoreMakesTheOutcomeUnknown`, `LostConfirmationAcknowledgementIsRecognizedOnRetry`, `UnrecordedConfirmationLeavesTheJobUnknownNotCompleted`, `test_lost_confirmation_is_unknown_not_completed`, checker `I8 completed job has unresolved external operation` |

## Related documents

- [Architecture](architecture.md)
- [Testing](testing.md)
- [ADR 0002: job state model and fencing rules](decisions/0002-job-state-model-and-fencing-rules.md)
- [Storage](storage.md)
- [ADR 0005: idempotent operations and UNKNOWN outcomes](decisions/0005-idempotent-operations-and-unknown-outcomes.md)
