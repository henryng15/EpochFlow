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

struct RenewResult {
  CommitStatus status = CommitStatus::kNotFound;
  // New lease deadline; meaningful only when status is kAccepted.
  TimePoint lease_until;
};

}  // namespace epochflow
