#pragma once

#include "core/job_store.h"
#include "store/pg_connection_pool.h"

#include <string>

namespace epochflow {

struct PgStoreOptions {
  // Connections shared by concurrent calls (gRPC handler threads, reclaimer).
  int pool_size = 4;
};

// Durable JobStore in PostgreSQL (invariant I6). Schema: sql/migrations; design
// and transaction notes: docs/storage.md.
//
// Every method is one transaction. State changes are single conditional
// UPDATEs whose WHERE clause carries the fencing check (job RUNNING with the
// caller's generation), so the database applies check and change atomically
// (I1-I3). Acquire claims with FOR UPDATE SKIP LOCKED, so concurrent acquirers,
// in this or other processes, never receive the same job. Lease deadlines are
// computed and compared with the database's now(); the coordinator host clock
// is never used.
//
// Errors: pqxx exceptions propagate. pqxx::broken_connection or
// pqxx::in_doubt_error mean the outcome is unknown; callers retry only
// idempotent operations (see JobStore).
//
// Thread-safe.
class PgJobStore final : public JobStore {
 public:
  // Opens one connection to fail fast. Throws pqxx::broken_connection if the
  // database is unreachable, and std::runtime_error if its schema is not at
  // LatestMigrationVersion() (run MigrateDatabase first).
  explicit PgJobStore(const std::string& dsn, PgStoreOptions options = {});

  JobId Submit(const JobSpec& spec) override;
  std::optional<JobRecord> Get(JobId id) const override;
  std::optional<Assignment> Acquire(const WorkerId& worker, Duration lease) override;
  RenewResult Renew(JobId id, Generation generation, Duration lease) override;
  CommitStatus Complete(JobId id, Generation generation, const std::string& result) override;
  CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                    bool retryable) override;
  OperationOutcome ApplyLedger(JobId id, Generation generation, const std::string& operation_id,
                               const LedgerEntry& entry) override;
  ExternalBegin BeginExternal(JobId id, Generation generation, const std::string& operation_id,
                              const std::string& request) override;
  CommitStatus ConfirmExternal(const std::string& operation_id, const std::string& request,
                               const std::string& confirmation) override;
  CommitStatus MarkUnknown(JobId id, Generation generation, const std::string& reason) override;
  CommitStatus Reconcile(JobId id, JobState outcome, const std::string& detail) override;
  CommitStatus ReconcileExternal(JobId id, const std::string& operation_id, JobState outcome,
                                 const std::string& evidence) override;
  std::size_t ReclaimExpired() override;

 private:
  mutable PgConnectionPool pool_;
};

}  // namespace epochflow
