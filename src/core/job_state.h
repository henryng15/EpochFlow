#pragma once

#include <string_view>

namespace epochflow {

// Lifecycle state of a job. The allowed transitions are the single source of
// truth for invariant I1 (docs/invariants.md); every store must apply state
// changes only through IsAllowedTransition.
//
//   PENDING -> RUNNING                      acquire (new generation)
//   RUNNING -> COMPLETED                    complete with current generation
//   RUNNING -> FAILED                       non-retryable failure, or attempts exhausted
//   RUNNING -> PENDING                      retryable failure or expired lease, attempts left
//   RUNNING -> UNKNOWN                      external effect outcome not confirmed
//   UNKNOWN -> COMPLETED | FAILED           reconciliation with evidence
enum class JobState { kPending, kRunning, kCompleted, kFailed, kUnknown };

// Upper-case names shared with the protocol and the database ("PENDING", ...).
std::string_view ToString(JobState state);

// COMPLETED and FAILED are final. UNKNOWN is not: it awaits reconciliation and
// is never retried automatically.
bool IsTerminal(JobState state);

bool IsAllowedTransition(JobState from, JobState to);

}  // namespace epochflow
