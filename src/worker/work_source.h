#pragma once

#include "core/clock.h"
#include "core/job.h"
#include "core/job_store.h"

#include <optional>
#include <string>

namespace epochflow {

// Where a Worker gets jobs, applies side effects, and reports outcomes: the
// coordinator over gRPC, or a JobStore directly for the in-process runner. The
// source, not the worker, decides lease length; each Assignment carries it.
//
// Status results have JobStore semantics (kStale: the assignment is no longer
// current). Transport or storage failures throw; the request may or may not
// have been applied. Everything except Acquire is idempotent for the same
// generation and content, so callers may retry those.
class WorkSource {
 public:
  virtual ~WorkSource() = default;
  // nullopt when no job is PENDING.
  virtual std::optional<Assignment> Acquire(const WorkerId& worker) = 0;
  virtual CommitStatus Renew(JobId id, Generation generation) = 0;
  virtual CommitStatus Complete(JobId id, Generation generation, const std::string& result) = 0;
  virtual CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                            bool retryable) = 0;
  virtual CommitStatus MarkUnknown(JobId id, Generation generation, const std::string& reason) = 0;

  // Side effects on behalf of the current assignment; JobStore semantics.
  virtual OperationOutcome ApplyLedger(JobId id, Generation generation,
                                       const std::string& operation_id,
                                       const LedgerEntry& entry) = 0;
  virtual ExternalBegin BeginExternal(JobId id, Generation generation,
                                      const std::string& operation_id,
                                      const std::string& request) = 0;
  virtual CommitStatus ConfirmExternal(const std::string& operation_id, const std::string& request,
                                       const std::string& confirmation) = 0;
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
  CommitStatus MarkUnknown(JobId id, Generation generation, const std::string& reason) override {
    return store_.MarkUnknown(id, generation, reason);
  }
  OperationOutcome ApplyLedger(JobId id, Generation generation, const std::string& operation_id,
                               const LedgerEntry& entry) override {
    return store_.ApplyLedger(id, generation, operation_id, entry);
  }
  ExternalBegin BeginExternal(JobId id, Generation generation, const std::string& operation_id,
                              const std::string& request) override {
    return store_.BeginExternal(id, generation, operation_id, request);
  }
  CommitStatus ConfirmExternal(const std::string& operation_id, const std::string& request,
                               const std::string& confirmation) override {
    return store_.ConfirmExternal(operation_id, request, confirmation);
  }

 private:
  JobStore& store_;
  Duration lease_;
};

}  // namespace epochflow
