#pragma once

#include "core/clock.h"
#include "core/job_store.h"

#include <map>
#include <mutex>

namespace epochflow {

// JobStore kept in process memory, guarded by one mutex. State is lost when the
// process exits, so it is not durable (invariant I6 needs the PostgreSQL store);
// it exists for the in-process runner and as the reference implementation of
// the JobStore contract.
//
// Lease expiry is judged with the injected clock, which must outlive the store.
// Acquire scans jobs in id order, so it is O(number of jobs): fine for tests
// and demos, not for large backlogs.
class InMemoryJobStore final : public JobStore {
 public:
  explicit InMemoryJobStore(const Clock& clock);

  JobId Submit(const JobSpec& spec) override;
  std::optional<JobRecord> Get(JobId id) const override;
  std::optional<Assignment> Acquire(const WorkerId& worker, Duration lease) override;
  RenewResult Renew(JobId id, Generation generation, Duration lease) override;
  CommitStatus Complete(JobId id, Generation generation, const std::string& result) override;
  CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                    bool retryable) override;
  OperationOutcome ApplyLedger(JobId id, Generation generation, const std::string& operation_id,
                               const LedgerEntry& entry) override;
  std::size_t ReclaimExpired() override;

 private:
  // One idempotent operation, keyed by operation_id (mirrors the operations
  // table of the PostgreSQL store).
  struct Operation {
    std::string kind;     // "LEDGER"
    std::string request;  // canonical content, compared on reuse
    JobId job_id = 0;
    Generation generation = 0;
    std::string result;   // ledger entry id
  };

  // kAccepted if `id` is RUNNING with `generation`; else kStale / kNotFound.
  // Requires mu_.
  CommitStatus CheckCurrent(JobId id, Generation generation) const;

  const Clock& clock_;
  mutable std::mutex mu_;
  // Ordered by id, which is submission order.
  std::map<JobId, JobRecord> jobs_;
  JobId next_id_ = 1;
  std::map<std::string, Operation> operations_;
  std::int64_t next_ledger_entry_ = 1;
};

}  // namespace epochflow
