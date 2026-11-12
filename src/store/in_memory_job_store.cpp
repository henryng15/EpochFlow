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
    return Assignment{id, job.generation, job.payload, *job.lease_until, job.attempts};
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
