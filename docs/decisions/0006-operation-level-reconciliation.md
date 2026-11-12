# 0006. Operation-level reconciliation of external effects

## Status

Accepted (2026-11-11). Amends [ADR 0005](0005-idempotent-operations-and-unknown-outcomes.md).

## Context

ADR 0005 parks a job as `UNKNOWN` when an external effect cannot be
confirmed, and lets an operator resolve it with `ReconcileJob`. That decision
updated only the `jobs` row. The external operation stayed `STARTED`, so:

- the database could show a `COMPLETED` job whose charge was never confirmed;
- any later attempt or job using the same `operation_id` still found an
  unconfirmed intent (`IN_DOUBT`) and became `UNKNOWN` again, although the
  operator had already established what the provider did.

Operators can also prove the opposite: the provider has no record of the
charge. Version 1 had no way to store that fact on the operation.

## Decision

1. **Reconciling an external job resolves its operation in the same
   transaction.** `ReconcileJobRequest` gains an optional `operation_id`
   (`epochflowctl reconcile JOB --operation=ID ...`). With it, the store locks
   the job and the operation and applies both changes, or neither:

   | Before | Decision | After |
   | --- | --- | --- |
   | job `UNKNOWN`, operation `STARTED` | completed, evidence *E* | operation `CONFIRMED` with result *E*; job `COMPLETED` with result *E* |
   | job `UNKNOWN`, operation `STARTED` | failed, evidence *E* | operation `FAILED` with result *E*; job `FAILED` with error *E* |
   | job `UNKNOWN`, operation already `CONFIRMED` with *C* (a confirmation landed late) | completed with *C* | job `COMPLETED`; operation unchanged |
   | job and operation already resolved this way, with *E* | the same decision | `OK`, nothing written |

   Every operator decision records `reconciled_at` on the operation.
2. **The operation must belong to the job:** kind `EXTERNAL` and created by
   that job (`operations.job_id`). An operation id shared by several jobs is
   reconciled through the job that created it.
3. **Job-only reconciliation is refused while the job owns an unresolved
   external operation** (`STARTED`), so the API cannot produce the divergence
   above. The operator is told to pass `--operation`.
4. **Negative evidence is terminal.** An operation reconciled as failed
   becomes `FAILED` and keeps the evidence. A later `BeginExternalOperation`
   for that id returns `EXTERNAL_STATE_FAILED` with the evidence, and the
   worker fails the job without retry. This mirrors provider idempotency keys,
   which replay a stored error: to charge again, submit with a new operation
   id. A provider confirmation for a `FAILED` operation is a conflict, which
   the worker turns into `UNKNOWN` (invariant I8).
5. **Contradictions are rejected without writing anything:** a different
   decision or different evidence for a resolved pair, completed evidence
   that differs from an existing confirmation, or a failed decision for a
   confirmed operation.

Status mapping over gRPC follows the existing error model: job not `UNKNOWN`,
or job-only reconcile with an unresolved operation, is `FAILED_PRECONDITION`;
an operation that is missing is `NOT_FOUND`; an operation that is not this
job's external operation, or conflicting evidence, is `ALREADY_EXISTS`.

## Consequences

- After a successful reconciliation, later attempts reuse the confirmed
  result (`EXTERNAL_STATE_CONFIRMED`) instead of becoming `UNKNOWN` again.
- The invariant checker can require agreement between jobs and their
  operations: a `COMPLETED` job owns no `STARTED` or `FAILED` external
  operation, and an operator-resolved operation matches its job's outcome
  (invariant I8).
- The runbook for external jobs needs the operation id, which the job's
  `UNKNOWN` reason already names.
- Schema migration `0004` adds the `FAILED` status and `reconciled_at`;
  existing rows stay valid.

## Alternatives considered

- **A separate `ReconcileExternalOperation` RPC:** clearer name, but a second
  operator command for the same decision. An optional field on the existing
  RPC is backward compatible and keeps one runbook entry.
- **Reconcile the operation automatically from job-only reconciliation:** the
  job does not say which operation it is about when it created several, and
  the evidence format would be guessed. Rejected; the operator names it.
- **Negative evidence re-opens the operation (`NOT_PERFORMED`, then a new
  intent may call the provider):** lets the same id charge later, but adds a
  state that is neither terminal nor in doubt and more paths to test. A new
  operation id gives the same result explicitly.
- **Delete the operation on negative evidence:** loses the audit trail and
  makes the id look never used.

## References

- [ADR 0005: idempotent operations and UNKNOWN outcomes](0005-idempotent-operations-and-unknown-outcomes.md)
- [Phase 2 plan](../phase-2.md)
- [Protocol](../protocol.md), [Storage](../storage.md), [Invariants](../invariants.md)
