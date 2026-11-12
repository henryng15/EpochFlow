#pragma once

#include "core/job_store.h"

namespace epochflow::testing {

// Delegates every call to another store. Test doubles derive from it and
// override only the behavior they change (for example rejecting renewals).
class ForwardingJobStore : public JobStore {
 public:
  explicit ForwardingJobStore(JobStore& inner) : inner_(inner) {}

  JobId Submit(const JobSpec& spec) override { return inner_.Submit(spec); }
  std::optional<JobRecord> Get(JobId id) const override { return inner_.Get(id); }
  std::optional<Assignment> Acquire(const WorkerId& w, Duration lease) override {
    return inner_.Acquire(w, lease);
  }
  RenewResult Renew(JobId id, Generation g, Duration lease) override {
    return inner_.Renew(id, g, lease);
  }
  CommitStatus Complete(JobId id, Generation g, const std::string& r) override {
    return inner_.Complete(id, g, r);
  }
  CommitStatus Fail(JobId id, Generation g, const std::string& r, bool retry) override {
    return inner_.Fail(id, g, r, retry);
  }
  OperationOutcome ApplyLedger(JobId id, Generation g, const std::string& op,
                               const LedgerEntry& e) override {
    return inner_.ApplyLedger(id, g, op, e);
  }
  ExternalBegin BeginExternal(JobId id, Generation g, const std::string& op,
                              const std::string& req) override {
    return inner_.BeginExternal(id, g, op, req);
  }
  CommitStatus ConfirmExternal(const std::string& op, const std::string& req,
                               const std::string& conf) override {
    return inner_.ConfirmExternal(op, req, conf);
  }
  CommitStatus MarkUnknown(JobId id, Generation g, const std::string& reason) override {
    return inner_.MarkUnknown(id, g, reason);
  }
  CommitStatus Reconcile(JobId id, JobState outcome, const std::string& detail) override {
    return inner_.Reconcile(id, outcome, detail);
  }
  std::size_t ReclaimExpired() override { return inner_.ReclaimExpired(); }

 protected:
  JobStore& inner_;
};

}  // namespace epochflow::testing
