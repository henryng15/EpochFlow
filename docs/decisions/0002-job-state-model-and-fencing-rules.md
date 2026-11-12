# 0002. Job state model, lease recovery, and fencing rules

## Status

Accepted (2026-11-11).

## Context

Workers can crash, pause, or lose contact while holding a job. The store must
let another worker take over without letting the old one overwrite the new
one's result, and retried requests (lost acknowledgements) must not change an
outcome. Several rules have more than one reasonable choice:

- What happens to a job whose lease expired: re-assign it directly, or move it
  back to a queue first?
- Is a result accepted after the lease deadline if nobody has taken over yet?
- What does a duplicate completion return?

## Decision

1. **Five states** with a fixed transition table (`src/core/job_state.cpp`):
   `PENDING -> RUNNING`; `RUNNING -> PENDING | COMPLETED | FAILED | UNKNOWN`;
   `UNKNOWN -> COMPLETED | FAILED`. `COMPLETED` and `FAILED` are terminal.
   `UNKNOWN` is not retried automatically.
2. **Generation is incremented only by acquire.** Every assignment therefore
   has a unique generation, and recovery itself never changes it.
3. **Expired leases go back through `PENDING`.** A recovery pass
   (`ReclaimExpired`) moves `RUNNING` jobs whose deadline is strictly in the
   past to `PENDING`, or to `FAILED` when no attempts remain. Acquire only takes
   `PENDING` jobs.
4. **Fencing checks state and generation, not time.** Renew, complete, and fail
   succeed only if the job is `RUNNING` with the caller's generation. A result
   that arrives after the deadline but before recovery is accepted: no newer
   assignment exists, so accepting it is safe and saves a re-run.
5. **Duplicates of the request that finished a job are idempotent.** Repeating
   complete (or final fail) with the same generation returns `kAccepted` when
   the content matches and `kConflict` when it differs; the stored outcome never
   changes. A duplicate of a retryable failure that re-queued the job returns
   `kStale`.
6. **Attempts count assignments.** A retryable failure or expiry re-queues only
   while `attempts < max_attempts`.

## Consequences

- One ordering rule covers every stale case: once a job leaves `RUNNING` or
  gains a newer generation, the old assignment can change nothing.
- Recovery adds up to one reclaim interval of latency before a job is re-run.
- `FAILED` from attempts exhausted by expiry and by handler errors look the
  same apart from the `error` text.
- Workers must stop and discard work when they cannot renew, because a
  reclaimed job may already be running elsewhere.
- A worker whose retryable-failure report is retried gets `kStale` and must
  treat it as "nothing to do", not as an error.

## Alternatives considered

- **Acquire directly takes expired `RUNNING` jobs:** removes the reclaim
  latency, but mixes recovery into the hot path and makes "attempts exhausted"
  handling and recovery tracing harder. Can be revisited if recovery latency
  matters.
- **Reject results after the lease deadline:** simpler to explain, but discards
  valid work without improving safety, since fencing already blocks real
  conflicts.
- **Duplicates return `kStale`:** a worker whose first acknowledgement was lost
  would wrongly believe it lost the job.
- **Fencing also on worker ID:** redundant; the generation already identifies
  one assignment.

## References

- [`src/core/job_store.h`](../../src/core/job_store.h): contract
- [`tests/contract/job_store_contract.cpp`](../../tests/contract/job_store_contract.cpp)
- [Invariants](../invariants.md)
