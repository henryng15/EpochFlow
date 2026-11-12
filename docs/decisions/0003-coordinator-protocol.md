# 0003. Coordinator-worker protocol

## Status

Accepted (2026-11-12).

## Context

Milestone 2 splits the in-process runner into a coordinator process and worker
processes. The protocol decides how work is handed out, who controls lease
timing, how store outcomes travel over gRPC, and what a worker does when a
call's outcome is unknown. These choices affect correctness under network
failures and are hard to change once clients exist.

## Decision

1. **Pull model with unary RPCs.** Workers call `AcquireJob`; `RenewLease` is
   the heartbeat. No streaming.
2. **The coordinator owns lease length** and returns it in each assignment.
   Workers renew every third of it using their own monotonic clock; absolute
   deadlines on the wire are informational.
3. **Store outcomes are gRPC status codes:** stale assignment is
   `FAILED_PRECONDITION`, conflicting duplicate is `ALREADY_EXISTS`, unknown job
   is `NOT_FOUND`. Storage failures map to `UNAVAILABLE` (outcome unknown).
4. **A worker that cannot confirm renewal treats the lease as lost**, cancels
   the task, and does not report.
5. **Result reports are retried** a bounded number of times after transport
   errors, relying on per-generation idempotency in the store.
6. **One `Worker` implementation** runs over a `WorkSource` interface, used by
   both the gRPC client and the in-process runner.
7. **Plaintext transport** for v1.

## Consequences

- Recovery and fencing logic is shared between local and distributed modes,
  so the in-process tests exercise the same worker code as production.
- Clock skew between machines cannot shorten or extend a lease from the
  worker's point of view.
- A brief network blip during renewal can abort a healthy job; it then re-runs
  after reclaim. Correctness is preferred over avoiding duplicate attempts.
- Idle workers poll; load grows with worker count and idle backoff.
- The API must not be exposed beyond trusted hosts until TLS and
  authentication exist.

## Alternatives considered

- **Server-streaming assignments (push):** lower latency and no polling, but
  needs connection tracking and flow control the project does not need yet.
- **Worker-chosen lease length:** lets a worker grant itself long leases and
  makes recovery time unpredictable.
- **Outcome enums inside response messages:** avoids overloading status codes,
  but every RPC would need two error channels (status and payload). Status codes
  already have matching meanings.
- **Tolerate N failed renewals before giving up:** fewer aborted jobs, but the
  worker would keep running without knowing whether it still holds the lease.

## References

- [Protocol](../protocol.md)
- [`proto/epochflow/v1/epochflow.proto`](../../proto/epochflow/v1/epochflow.proto)
- [ADR 0002: job state model and fencing rules](0002-job-state-model-and-fencing-rules.md)
