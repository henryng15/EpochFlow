#pragma once

#include "core/clock.h"
#include "core/job.h"
#include "core/job_store.h"

#include <optional>
#include <string>

namespace epochflow {

// Where a Worker gets jobs and reports outcomes: the coordinator over gRPC, or
// a JobStore directly for the in-process runner. The source, not the worker,
// decides lease length; each Assignment carries it.
//
// Status results have JobStore semantics (kStale: the assignment is no longer
// current). Transport or storage failures throw; the request may or may not
// have been applied, so callers retry only operations that are idempotent for
// the same generation (Renew, Complete, Fail).
class WorkSource {
 public:
  virtual ~WorkSource() = default;
  // nullopt when no job is PENDING.
  virtual std::optional<Assignment> Acquire(const WorkerId& worker) = 0;
  virtual CommitStatus Renew(JobId id, Generation generation) = 0;
  virtual CommitStatus Complete(JobId id, Generation generation, const std::string& result) = 0;
  virtual CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                            bool retryable) = 0;
};

// WorkSource backed by a JobStore in the same process, granting `lease` on
// every acquire and renewal. The store must outlive this object.
class StoreWorkSource final : public WorkSource {
 public:
  StoreWorkSource(JobStore& store, Duration lease) : store_(store), lease_(lease) {}

  std::optional<Assignment> Acquire(const WorkerId& worker) override {
    return store_.Acquire(worker, lease_);
  }
  CommitStatus Renew(JobId id, Generation generation) override {
    return store_.Renew(id, generation, lease_).status;
  }
  CommitStatus Complete(JobId id, Generation generation, const std::string& result) override {
    return store_.Complete(id, generation, result);
  }
  CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                    bool retryable) override {
    return store_.Fail(id, generation, reason, retryable);
  }

 private:
  JobStore& store_;
  Duration lease_;
};

}  // namespace epochflow
