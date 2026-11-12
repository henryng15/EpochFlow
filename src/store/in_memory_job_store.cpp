#include "store/in_memory_job_store.h"

#include <stdexcept>
#include <string>

namespace epochflow {
namespace {

// Applies a state change only if the transition table allows it (invariant
// I1). A violation is a bug in this store, not a caller error.
void TransitionTo(JobRecord& job, JobState to) {
  if (!IsAllowedTransition(job.state, to)) {
    throw std::logic_error("illegal job transition " + std::string(ToString(job.state)) +
                           " -> " + std::string(ToString(to)));
  }
  job.state = to;
}

void ReleaseLease(JobRecord& job) {
  job.worker_id.reset();
  job.lease_until.reset();
}

bool IsCurrentAssignment(const JobRecord& job, Generation generation) {
  return job.state == JobState::kRunning && job.generation == generation;
}

void ValidateLease(Duration lease) {
  if (lease <= Duration::zero()) throw std::invalid_argument("lease must be positive");
}

}  // namespace

InMemoryJobStore::InMemoryJobStore(const Clock& clock) : clock_(clock) {}

JobId InMemoryJobStore::Submit(const JobSpec& spec) {
  if (spec.max_attempts < 1) throw std::invalid_argument("max_attempts must be >= 1");
  std::lock_guard lock(mu_);
  const JobId id = next_id_++;
  JobRecord job;
  job.id = id;
  job.max_attempts = spec.max_attempts;
  job.payload = spec.payload;
  jobs_.emplace(id, std::move(job));
  return id;
}

std::optional<JobRecord> InMemoryJobStore::Get(JobId id) const {
  std::lock_guard lock(mu_);
  const auto it = jobs_.find(id);
  if (it == jobs_.end()) return std::nullopt;
  return it->second;
}

std::optional<Assignment> InMemoryJobStore::Acquire(const WorkerId& worker, Duration lease) {
  ValidateLease(lease);
  std::lock_guard lock(mu_);
  for (auto& [id, job] : jobs_) {
    if (job.state != JobState::kPending) continue;
    // Claim and generation bump happen under one lock, so no two callers can
    // hold the same generation (invariant I2).
    TransitionTo(job, JobState::kRunning);
    ++job.generation;
    ++job.attempts;
    job.worker_id = worker;
    job.lease_until = clock_.Now() + lease;
    return Assignment{id, job.generation, job.payload, *job.lease_until, lease, job.attempts};
  }
  return std::nullopt;
}

RenewResult InMemoryJobStore::Renew(JobId id, Generation generation, Duration lease) {
  ValidateLease(lease);
  std::lock_guard lock(mu_);
  const auto it = jobs_.find(id);
  if (it == jobs_.end()) return {CommitStatus::kNotFound, {}};
  JobRecord& job = it->second;
  if (!IsCurrentAssignment(job, generation)) return {CommitStatus::kStale, {}};
  job.lease_until = clock_.Now() + lease;
  return {CommitStatus::kAccepted, *job.lease_until};
}

CommitStatus InMemoryJobStore::Complete(JobId id, Generation generation,
                                        const std::string& result) {
  std::lock_guard lock(mu_);
  const auto it = jobs_.find(id);
  if (it == jobs_.end()) return CommitStatus::kNotFound;
  JobRecord& job = it->second;
  // The generation never changes after a terminal transition, so a match here
  // means this assignment is the one that completed the job.
  if (job.state == JobState::kCompleted && job.generation == generation) {
    return job.result == result ? CommitStatus::kAccepted : CommitStatus::kConflict;
  }
  if (!IsCurrentAssignment(job, generation)) return CommitStatus::kStale;
  TransitionTo(job, JobState::kCompleted);
  job.result = result;
  ReleaseLease(job);
  return CommitStatus::kAccepted;
}

CommitStatus InMemoryJobStore::Fail(JobId id, Generation generation, const std::string& reason,
                                    bool retryable) {
  std::lock_guard lock(mu_);
  const auto it = jobs_.find(id);
  if (it == jobs_.end()) return CommitStatus::kNotFound;
  JobRecord& job = it->second;
  if (job.state == JobState::kFailed && job.generation == generation) {
    return job.error == reason ? CommitStatus::kAccepted : CommitStatus::kConflict;
  }
  if (!IsCurrentAssignment(job, generation)) return CommitStatus::kStale;
  const bool requeue = retryable && job.attempts < job.max_attempts;
  TransitionTo(job, requeue ? JobState::kPending : JobState::kFailed);
  job.error = reason;
  ReleaseLease(job);
  return CommitStatus::kAccepted;
}

CommitStatus InMemoryJobStore::CheckCurrent(JobId id, Generation generation) const {
  const auto it = jobs_.find(id);
  if (it == jobs_.end()) return CommitStatus::kNotFound;
  return IsCurrentAssignment(it->second, generation) ? CommitStatus::kAccepted
                                                     : CommitStatus::kStale;
}

OperationOutcome InMemoryJobStore::ApplyLedger(JobId id, Generation generation,
                                               const std::string& operation_id,
                                               const LedgerEntry& entry) {
  if (operation_id.empty()) throw std::invalid_argument("operation_id is required");
  if (entry.account.empty()) throw std::invalid_argument("account is required");
  const std::string request = entry.account + "\n" + std::to_string(entry.amount_cents);
  std::lock_guard lock(mu_);
  // Fencing first, under the same lock as the effect (invariant I3).
  if (const CommitStatus s = CheckCurrent(id, generation); s != CommitStatus::kAccepted) {
    return {s, {}, false};
  }
  if (const auto it = operations_.find(operation_id); it != operations_.end()) {
    const Operation& op = it->second;
    if (op.kind != "LEDGER" || op.request != request) return {CommitStatus::kConflict, {}, false};
    return {CommitStatus::kAccepted, op.result, true};
  }
  const std::string entry_id = std::to_string(next_ledger_entry_++);
  operations_.emplace(operation_id,
                      Operation{"LEDGER", request, "APPLIED", id, generation, entry_id});
  return {CommitStatus::kAccepted, entry_id, false};
}

ExternalBegin InMemoryJobStore::BeginExternal(JobId id, Generation generation,
                                              const std::string& operation_id,
                                              const std::string& request) {
  if (operation_id.empty()) throw std::invalid_argument("operation_id is required");
  std::lock_guard lock(mu_);
  if (const CommitStatus s = CheckCurrent(id, generation); s != CommitStatus::kAccepted) {
    return {s, ExternalState::kStart, {}};
  }
  const auto it = operations_.find(operation_id);
  if (it == operations_.end()) {
    operations_.emplace(operation_id,
                        Operation{"EXTERNAL", request, "STARTED", id, generation, {}});
    return {CommitStatus::kAccepted, ExternalState::kStart, {}};
  }
  const Operation& op = it->second;
  if (op.kind != "EXTERNAL" || op.request != request) {
    return {CommitStatus::kConflict, ExternalState::kStart, {}};
  }
  if (op.status == "CONFIRMED") return {CommitStatus::kAccepted, ExternalState::kConfirmed, op.result};
  if (op.status == "FAILED") return {CommitStatus::kAccepted, ExternalState::kFailed, op.result};
  const bool same_attempt = op.job_id == id && op.generation == generation;
  return {CommitStatus::kAccepted, same_attempt ? ExternalState::kStart : ExternalState::kInDoubt,
          {}};
}

CommitStatus InMemoryJobStore::ConfirmExternal(const std::string& operation_id,
                                               const std::string& request,
                                               const std::string& confirmation) {
  std::lock_guard lock(mu_);
  const auto it = operations_.find(operation_id);
  if (it == operations_.end()) return CommitStatus::kNotFound;
  Operation& op = it->second;
  if (op.kind != "EXTERNAL" || op.request != request) return CommitStatus::kConflict;
  if (op.status == "CONFIRMED") {
    return op.result == confirmation ? CommitStatus::kAccepted : CommitStatus::kConflict;
  }
  // An operator proved the effect never happened; a confirmation contradicts it.
  if (op.status == "FAILED") return CommitStatus::kConflict;
  op.status = "CONFIRMED";
  op.result = confirmation;
  return CommitStatus::kAccepted;
}

CommitStatus InMemoryJobStore::MarkUnknown(JobId id, Generation generation,
                                           const std::string& reason) {
  std::lock_guard lock(mu_);
  const auto it = jobs_.find(id);
  if (it == jobs_.end()) return CommitStatus::kNotFound;
  JobRecord& job = it->second;
  if (job.state == JobState::kUnknown && job.generation == generation) {
    return job.error == reason ? CommitStatus::kAccepted : CommitStatus::kConflict;
  }
  if (!IsCurrentAssignment(job, generation)) return CommitStatus::kStale;
  TransitionTo(job, JobState::kUnknown);
  job.error = reason;
  ReleaseLease(job);
  return CommitStatus::kAccepted;
}

CommitStatus InMemoryJobStore::Reconcile(JobId id, JobState outcome, const std::string& detail) {
  if (outcome != JobState::kCompleted && outcome != JobState::kFailed) {
    throw std::invalid_argument("reconcile outcome must be COMPLETED or FAILED");
  }
  std::lock_guard lock(mu_);
  const auto it = jobs_.find(id);
  if (it == jobs_.end()) return CommitStatus::kNotFound;
  JobRecord& job = it->second;
  if (job.state == JobState::kUnknown) {
    TransitionTo(job, outcome);
    (outcome == JobState::kCompleted ? job.result : job.error) = detail;
    return CommitStatus::kAccepted;
  }
  // Repeating the decision that resolved the job is idempotent.
  const auto& recorded = outcome == JobState::kCompleted ? job.result : job.error;
  return job.state == outcome && recorded == detail ? CommitStatus::kAccepted
                                                    : CommitStatus::kStale;
}

CommitStatus InMemoryJobStore::ReconcileExternal(JobId id, const std::string& operation_id,
                                                 JobState outcome, const std::string& evidence) {
  if (operation_id.empty()) throw std::invalid_argument("operation_id is required");
  if (outcome != JobState::kCompleted && outcome != JobState::kFailed) {
    throw std::invalid_argument("reconcile outcome must be COMPLETED or FAILED");
  }
  const bool completed = outcome == JobState::kCompleted;
  const std::string resolved = completed ? "CONFIRMED" : "FAILED";
  // Everything below runs under one lock: both records change, or neither.
  std::lock_guard lock(mu_);
  const auto job_it = jobs_.find(id);
  if (job_it == jobs_.end()) return CommitStatus::kNotFound;
  const auto op_it = operations_.find(operation_id);
  if (op_it == operations_.end()) return CommitStatus::kNotFound;
  JobRecord& job = job_it->second;
  Operation& op = op_it->second;
  if (op.kind != "EXTERNAL" || op.job_id != id) return CommitStatus::kConflict;

  if (job.state != JobState::kUnknown) {
    // Repeating the decision that resolved the pair is idempotent.
    const auto& recorded = completed ? job.result : job.error;
    const bool same = job.state == outcome && recorded == evidence && op.status == resolved &&
                      op.result == evidence;
    return same ? CommitStatus::kAccepted : CommitStatus::kStale;
  }
  if (op.status == "STARTED") {
    op.status = resolved;
    op.result = evidence;
  } else if (!(op.status == "CONFIRMED" && completed && op.result == evidence)) {
    // The evidence contradicts what is already recorded for the operation.
    return CommitStatus::kConflict;
  }
  TransitionTo(job, outcome);
  (completed ? job.result : job.error) = evidence;
  return CommitStatus::kAccepted;
}

std::size_t InMemoryJobStore::ReclaimExpired() {
  std::lock_guard lock(mu_);
  const TimePoint now = clock_.Now();
  std::size_t reclaimed = 0;
  for (auto& [id, job] : jobs_) {
    if (job.state != JobState::kRunning || !(now > *job.lease_until)) continue;
    const bool requeue = job.attempts < job.max_attempts;
    TransitionTo(job, requeue ? JobState::kPending : JobState::kFailed);
    job.error = requeue ? "lease expired" : "lease expired; no attempts left";
    ReleaseLease(job);
    ++reclaimed;
  }
  return reclaimed;
}

}  // namespace epochflow
