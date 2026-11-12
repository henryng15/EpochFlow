# Protocol

## Purpose

Semantics of the `epochflow.v1.Coordinator` gRPC API: what each RPC does,
which errors it returns, and which calls are safe to retry. The schema is
[`proto/epochflow/v1/epochflow.proto`](../proto/epochflow/v1/epochflow.proto).

## Context and terminology

- **Client:** submits and inspects jobs (`epochflowctl`).
- **Worker:** pulls work, renews leases, reports outcomes (`epochflow-worker`).
- **Generation:** fencing token returned by `AcquireJob`; every later request
  about that assignment must carry it.
- **Ambiguous outcome:** the client got no answer (timeout, connection loss),
  so the request may or may not have been applied.

## Design

Unary RPCs only; workers **pull** (`AcquireJob`) and `RenewLease` doubles as
the heartbeat. The coordinator chooses the lease length and returns it as
`Assignment.lease_ms`; workers renew every `lease_ms / 3`, measured on their
own monotonic clock, so worker and coordinator clocks need not agree.

| RPC | Effect | Retry after ambiguous outcome |
| --- | --- | --- |
| `SubmitJob` | New `PENDING` job; `max_attempts` 0 means default 3 | **Not idempotent**: may create a duplicate job |
| `GetJob` | Read a job snapshot | Safe |
| `AcquireJob` | Oldest `PENDING` job becomes `RUNNING` with generation + 1; empty response if none | May assign a second job; the orphan is reclaimed after its lease |
| `RenewLease` | Lease deadline = now + lease | Safe |
| `CompleteJob` | `RUNNING` to `COMPLETED` | Safe: a duplicate with the same result returns `OK` |
| `FailJob` | Re-queue (retryable, attempts left) or `FAILED` | Safe: final failure duplicate returns `OK`; re-queued duplicate returns `FAILED_PRECONDITION` |
| `ApplyLedgerOperation` | Ledger entry once per `operation_id` (fenced); returns `entry_id`, `deduplicated` | Safe: returns the original entry |
| `BeginExternalOperation` | Records intent (fenced); returns `START`, `IN_DOUBT`, or `CONFIRMED` (+ confirmation) | Safe within the same attempt (`START` again) |
| `ConfirmExternalOperation` | Stores the provider's confirmation (not fenced) | Safe: same confirmation returns `OK` |
| `MarkJobUnknown` | `RUNNING` to `UNKNOWN` (fenced) | Safe: same reason returns `OK` |
| `ReconcileJob` | Operator: `UNKNOWN` to `COMPLETED`/`FAILED` with `detail` | Safe: same decision returns `OK` |

**External operation states:** `START` means call the provider, then confirm;
`IN_DOUBT` means another attempt recorded the intent and never confirmed, so
the worker must not call the provider and reports `UNKNOWN` instead;
`CONFIRMED` returns the earlier answer for reuse.

### Error model

| Code | Meaning | Worker action |
| --- | --- | --- |
| `OK` | Applied, or identical duplicate recognized | Continue |
| `INVALID_ARGUMENT` | Malformed request (negative `max_attempts`, empty `worker_id` or `operation_id`, non-terminal reconcile outcome) | Fix the request |
| `NOT_FOUND` | Unknown job id | Drop |
| `FAILED_PRECONDITION` | Stale assignment: job not `RUNNING` with this generation (reconcile: job not `UNKNOWN`) | **Stop work, discard result** |
| `ALREADY_EXISTS` | Duplicate of the finishing request with different content, or an `operation_id` reused with different content or kind | Drop; outcome unchanged; for operations, fail the job without retry |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL` | Outcome unknown | Retry idempotent calls with the same generation |

## Invariants and failure semantics

- **Fencing is decided by the store**, not by the coordinator process: each
  RPC is a single atomic store call (invariant I3).
- **Lost renewal = lost lease.** If `RenewLease` returns `FAILED_PRECONDITION`
  or fails in transport, the worker cancels the task and never reports. This
  is conservative: a transient network error can cost a retry, but a worker
  never keeps working without a confirmed lease.
- **Lost report is retried.** `CompleteJob`/`FailJob` are retried (default 3
  tries) after transport errors. Because duplicates are idempotent per
  generation, a retry after a lost acknowledgement returns `OK` instead of
  looking stale. If every try fails, the job stays `RUNNING` until its lease
  expires and is reclaimed, then runs again.
- **Duplicate attempts are possible** (crash after work, before report). Their
  side effects are not duplicated: ledger effects go through
  `ApplyLedgerOperation` (deduplicated by `operation_id`), and external calls
  are bracketed by `BeginExternalOperation`/`ConfirmExternalOperation` so an
  attempt never repeats a call whose outcome is unknown.
- **Uncertainty is reported, not guessed.** A provider timeout or an
  unconfirmed earlier intent becomes `MarkJobUnknown`; the job waits for
  `ReconcileJob`.

## Implementation map

| Path | Contents |
| --- | --- |
| `proto/epochflow/v1/epochflow.proto` | Schema and per-RPC comments |
| `src/coordinator/coordinator_service.*` | Server: one store call per RPC, exception-to-status mapping |
| `src/coordinator/coordinator.*` | gRPC server plus `Reclaimer` |
| `src/client/coordinator_client.*` | Client; implements `WorkSource`; throws `RpcError` |
| `src/rpc/convert.*` | Domain/proto conversion and status mapping, shared by both sides |
| `src/worker/worker.*` | Renewal cadence, lost-lease handling, report retries |

## Operating and testing

`tests/grpc/coordinator_grpc_test.cpp` checks every status mapping over a real
loopback connection; `tests/grpc/distributed_test.cpp` and
`tests/e2e/smoke_test.sh` check recovery of a crashed worker. See
[testing.md](testing.md).

## Trade-offs and limitations

- **Plaintext, unauthenticated transport.** Any client that reaches the port
  can submit, acquire, or complete jobs. Bind to loopback or a trusted network.
  Upgrade trigger: any deployment beyond one trusted host (add TLS and worker
  credentials).
- `SubmitJob` has no idempotency key; a client retry can duplicate a job.
- Polling: idle workers call `AcquireJob` every `idle-backoff-ms`. Fine for a
  handful of workers; a long-poll or stream would be needed for many.
- Timestamps on the wire are Unix milliseconds (`int64`), for display only.

## Related documents

- [Architecture](architecture.md)
- [Invariants](invariants.md)
- [Operations](operations.md)
- [ADR 0003: coordinator protocol](decisions/0003-coordinator-protocol.md)
- [ADR 0005: idempotent operations and UNKNOWN outcomes](decisions/0005-idempotent-operations-and-unknown-outcomes.md)
