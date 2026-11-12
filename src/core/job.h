#pragma once

#include "core/clock.h"
#include "core/job_state.h"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace epochflow {

using JobId = std::int64_t;
// Fencing token: incremented on every assignment of a job, never reused.
using Generation = std::int64_t;
using WorkerId = std::string;

// Input for a new job.
struct JobSpec {
  std::string payload;
  // Total executions allowed, including the first. Must be >= 1.
  int max_attempts = 3;
};

// Snapshot of a job as stored. Copies are not kept in sync with the store.
struct JobRecord {
  JobId id = 0;
  JobState state = JobState::kPending;
  // 0 until the first assignment.
  Generation generation = 0;
  // Holder of the current lease; set only while RUNNING.
  std::optional<WorkerId> worker_id;
  // Lease deadline; set only while RUNNING.
  std::optional<TimePoint> lease_until;
  // Assignments started so far.
  int attempts = 0;
  int max_attempts = 0;
  std::string payload;
  // Set when COMPLETED.
  std::optional<std::string> result;
  // Reason for the most recent failure or recovery, if any.
  std::optional<std::string> error;
};

// What a worker receives from a successful acquire. The generation must be
// sent back with every renewal and result for this assignment.
struct Assignment {
  JobId job_id = 0;
  Generation generation = 0;
  std::string payload;
  TimePoint lease_until;
  // Length of the granted lease. Workers derive their renewal interval from it
  // rather than from lease_until, so their clocks need not match the store's.
  Duration lease{};
  // 1-based attempt number of this assignment.
  int attempt = 0;
};

// Outcome of a generation-checked request (renew, complete, fail).
enum class CommitStatus {
  kAccepted,
  kNotFound,
  // The caller's assignment is no longer current: the job has a newer
  // generation or is not RUNNING. The caller must discard its work.
  kStale,
  // Duplicate of an already applied request for the same generation, but with
  // different content. Nothing was changed.
  kConflict,
};

std::string_view ToString(CommitStatus status);

// An entry in the ledger owned by EpochFlow: the demonstration side effect
// whose idempotency the store guarantees (invariant I4).
struct LedgerEntry {
  std::string account;
  std::int64_t amount_cents = 0;
};

// Outcome of an idempotent operation (see JobStore::ApplyLedger).
struct OperationOutcome {
  CommitStatus status = CommitStatus::kNotFound;
  // Ledger entry id when status is kAccepted.
  std::string result;
  // True when the operation already existed and its earlier result was
  // returned instead of creating another effect.
  bool deduplicated = false;
};

// What the caller of BeginExternal must do next.
enum class ExternalState {
  // The intent is recorded for this attempt: call the provider, then confirm.
  kStart,
  // An earlier attempt recorded the intent and never confirmed; it may or may
  // not have reached the provider. Do not call it again: report UNKNOWN.
  kInDoubt,
  // An earlier call was confirmed; `result` holds the provider's answer.
  kConfirmed,
  // An operator established that the provider never performed the effect
  // (ADR 0006); `result` holds that evidence. Do not call the provider: the
  // job fails without retry. Charging again needs a new operation id.
  kFailed,
};

std::string_view ToString(ExternalState state);

struct ExternalBegin {
  CommitStatus status = CommitStatus::kNotFound;
  // Meaningful only when status is kAccepted.
  ExternalState state = ExternalState::kStart;
  // Provider confirmation when state is kConfirmed; the operator's evidence
  // when state is kFailed.
  std::string result;
};

struct RenewResult {
  CommitStatus status = CommitStatus::kNotFound;
  // New lease deadline; meaningful only when status is kAccepted.
  TimePoint lease_until;
};

}  // namespace epochflow
