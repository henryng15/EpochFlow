# Invariants

## Purpose

Lists the safety properties EpochFlow must preserve under failures and where
each one is enforced and tested. Each entry is updated when its enforcement code
lands.

## Context and terminology

See [architecture.md](architecture.md) for job, lease, and generation.

## Invariants and failure semantics

| ID | Invariant | Enforcement | Status |
| --- | --- | --- | --- |
| I1 | A job changes state only along an allowed transition. | Transition table, then conditional SQL updates | Planned (M1, M3) |
| I2 | At most one assignment of a job holds a valid lease at any time. | Atomic claim that increments the generation | Planned (M1, M3) |
| I3 | A result, renewal, or side effect is accepted only for the job's current generation. | Generation check in the same atomic update | Planned (M1, M3) |
| I4 | One `operation_id` produces at most one logical side effect. | Unique key in the same transaction as the effect | Planned (M4) |
| I5 | An unconfirmed external effect is recorded as `UNKNOWN`, never silently retried or marked failed. | Worker result reporting and reconciliation | Planned (M4) |
| I6 | Committed job state survives a coordinator restart. | PostgreSQL is the only state store | Planned (M3) |

An expired lease permits recovery; it does not prove the previous worker
stopped. I3 is what makes recovery safe while that worker may still be running.

## Implementation map

Filled in as each invariant is implemented.

## Related documents

- [Architecture](architecture.md)
- [Testing](testing.md)
