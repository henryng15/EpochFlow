#pragma once

#include "core/clock.h"
#include "core/job.h"
#include "epochflow/v1/epochflow.grpc.pb.h"
#include "worker/work_source.h"

#include <chrono>
#include <memory>
#include <optional>
#include <string>

namespace epochflow {

struct ClientOptions {
  // Deadline applied to every RPC.
  Duration rpc_timeout = std::chrono::seconds{5};
};

// Blocking client for epochflow.v1.Coordinator. Implements WorkSource, so a
// Worker can run against a remote coordinator unchanged.
//
// Store outcomes come back as CommitStatus. Everything else (unreachable
// coordinator, timeout, INVALID_ARGUMENT, server errors) throws RpcError; for
// transport errors the request may or may not have been applied (see the
// retry notes in epochflow.proto).
//
// Thread safety: safe for concurrent use (one gRPC channel, per-call contexts).
// Connections are made lazily, so construction never fails on reachability.
class CoordinatorClient final : public WorkSource {
 public:
  // `target` is a gRPC target such as "127.0.0.1:50051". Plaintext transport.
  explicit CoordinatorClient(const std::string& target, ClientOptions options = {});

  // Not idempotent: a retry after a lost response may create a second job.
  // max_attempts 0 lets the coordinator pick its default.
  JobId Submit(const JobSpec& spec);
  // nullopt for an unknown job.
  std::optional<JobRecord> Get(JobId id);

  std::optional<Assignment> Acquire(const WorkerId& worker) override;
  CommitStatus Renew(JobId id, Generation generation) override;
  CommitStatus Complete(JobId id, Generation generation, const std::string& result) override;
  CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                    bool retryable) override;
  CommitStatus MarkUnknown(JobId id, Generation generation, const std::string& reason) override;
  OperationOutcome ApplyLedger(JobId id, Generation generation, const std::string& operation_id,
                               const LedgerEntry& entry) override;
  ExternalBegin BeginExternal(JobId id, Generation generation, const std::string& operation_id,
                              const std::string& request) override;
  CommitStatus ConfirmExternal(const std::string& operation_id, const std::string& request,
                               const std::string& confirmation) override;

  // Operator action (epochflowctl reconcile). kStale if the job is not UNKNOWN.
  CommitStatus Reconcile(JobId id, JobState outcome, const std::string& detail);

 private:
  void Prepare(grpc::ClientContext& context) const;

  std::unique_ptr<v1::Coordinator::Stub> stub_;
  ClientOptions options_;
};

}  // namespace epochflow
