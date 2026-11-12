# 0005. Idempotent operations, external intents, and UNKNOWN outcomes

## Status

Accepted (2026-11-12). Amended by [ADR 0006](0006-operation-level-reconciliation.md): reconciliation also resolves the external operation.

## Context

Fencing (ADR 0002) stops a stale attempt from committing a result, but it
does not stop the same logical side effect from happening twice: a worker can
perform an effect and crash before reporting, and the retry performs it again.
Two kinds of effects need different answers:

- **Owned effects** live in EpochFlow's own database (the demonstration
  ledger). They can be made exactly-once per logical operation.
- **External effects** go to a third party without idempotency keys (the
  simulated card provider). A lost response, or a crash after the call,
  leaves the outcome unknown, and retrying may charge twice.

## Decision

1. **Callers name each logical effect with a stable `operation_id`** (from the
   payload, default `job-<id>`), shared by every attempt and by duplicate
   submissions.
2. **Owned effects are applied by the store in one transaction** that checks
   the caller's assignment while holding a share lock on the job row, records
   the operation, and creates the ledger entry. A repeated `operation_id` with
   the same content returns the original entry; different content is a
   conflict. Fencing is checked first, so a stale attempt changes nothing.
3. **External effects are preceded by a recorded intent** (`STARTED`) and
   followed by a recorded confirmation (`CONFIRMED`). An attempt that finds
   another attempt's unconfirmed intent must not call the provider; it reports
   `UNKNOWN`. A provider timeout also reports `UNKNOWN`.
4. **Confirmations are accepted from any attempt**, even a stale one: they are
   facts about the outside world and let later attempts finish instead of
   becoming `UNKNOWN`.
5. **`UNKNOWN` is parked**, not terminal: no retry, no reclaim, no acquire.
   Only an operator `ReconcileJob` with outside evidence moves it to
   `COMPLETED` or `FAILED`.
6. **Every assignment is traced in `job_attempts`**, and
   `scripts/check-invariants.sql` checks the safety properties from data at
   rest; fault-injection tests and every PostgreSQL contract test run it.

## Consequences

- A worker crash after an owned effect costs a re-run but never a duplicate
  entry, including when two jobs were submitted for the same operation.
- External effects are at most once, but not exactly once: an attempt that
  crashes between recording the intent and calling the provider leaves an
  `UNKNOWN` job that, on inspection, was never charged. Uncertainty becomes
  visible work for an operator instead of a silent duplicate.
- A lost `BeginExternal` acknowledgement followed by a crash also produces
  `UNKNOWN` (conservative); retrying `BeginExternal` within the same attempt
  is safe and returns `START` again.
- Reconciliation is manual in v1 (`epochflowctl reconcile`); the provider's own
  record (the provider log) is the evidence.
- Operation ids are global: reusing one for unrelated work is a conflict.

## Alternatives considered

- **Retry external calls on timeout:** simple, but charges twice whenever the
  first call succeeded.
- **Mark timeouts `FAILED`:** claims a failure without evidence and invites a
  manual retry that double-charges.
- **Fence confirmations:** would discard true information from a paused
  worker and turn a finished charge into `UNKNOWN`.
- **Request hashes instead of stored request text:** smaller rows, but a hash
  collision would silently merge different requests; requests are small.
- **Automatic reconciliation against the provider:** needs a provider lookup
  API; the file provider is a stand-in, so v1 keeps the decision with the
  operator.

## References

- [Storage](../storage.md), [Protocol](../protocol.md), [Invariants](../invariants.md)
- [ADR 0002: job state model and fencing rules](0002-job-state-model-and-fencing-rules.md)
- [`tests/faults/test_faults.py`](../../tests/faults/test_faults.py)
