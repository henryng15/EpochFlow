#pragma once

#include "core/clock.h"
#include "core/job.h"

#include <cstddef>
#include <optional>
#include <string>

namespace epochflow {

// Durable home of job state, leases, and generations. Every method is one
// atomic state change: concurrent callers observe either all of it or none.
// Implementations must be safe to call from multiple threads.
//
// Fencing (invariant I3): renew, complete, and fail succeed only when the job
// is RUNNING with exactly the caller's generation. An expired lease alone does
// not make a request stale; ReclaimExpired does, by moving the job out of
// RUNNING. After that, only a new Acquire (with a higher generation) can run it.
//
// Errors: invalid arguments throw std::invalid_argument. Storage failures
// throw; the change is then either fully applied or not at all, and the caller
// may not know which (callers retry with the same generation, which is safe
// because duplicate completions and failures are idempotent).
class JobStore {
 public:
  virtual ~JobStore() = default;

  // Creates a PENDING job with generation 0. Throws std::invalid_argument if
  // spec.max_attempts < 1.
  virtual JobId Submit(const JobSpec& spec) = 0;

  virtual std::optional<JobRecord> Get(JobId id) const = 0;

  // Assigns the oldest PENDING job to `worker`: PENDING -> RUNNING, generation
  // and attempts incremented, lease set to now + `lease`. Returns nullopt when
  // no job is PENDING. Concurrent callers never receive the same assignment.
  // Throws std::invalid_argument if lease <= 0.
  virtual std::optional<Assignment> Acquire(const WorkerId& worker, Duration lease) = 0;

  // Extends the lease to now + `lease` (also after the deadline passed, as long
  // as the job was not reclaimed). kStale if the assignment is not current.
  virtual RenewResult Renew(JobId id, Generation generation, Duration lease) = 0;

  // RUNNING -> COMPLETED with `result`. A duplicate for the generation that
  // completed the job returns kAccepted if `result` matches, else kConflict;
  // the stored result never changes.
  virtual CommitStatus Complete(JobId id, Generation generation,
                                const std::string& result) = 0;

  // Records a failed attempt. Retryable with attempts left: RUNNING -> PENDING.
  // Otherwise RUNNING -> FAILED. A duplicate for the generation that failed the
  // job returns kAccepted if `reason` matches, else kConflict. A duplicate of a
  // retryable failure that re-queued the job returns kStale.
  virtual CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                            bool retryable) = 0;

  // Recovers every RUNNING job whose lease deadline is strictly in the past:
  // PENDING if attempts remain, otherwise FAILED. The generation is unchanged;
  // the next Acquire increments it. Returns the number of jobs recovered.
  // Recovery is permission to re-run, not proof the old worker stopped.
  virtual std::size_t ReclaimExpired() = 0;
};

}  // namespace epochflow
