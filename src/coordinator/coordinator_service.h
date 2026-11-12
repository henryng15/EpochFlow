#pragma once

#include "common/log.h"
#include "core/clock.h"
#include "core/job_store.h"
#include "epochflow/v1/epochflow.grpc.pb.h"

namespace epochflow {

// gRPC implementation of epochflow.v1.Coordinator over a JobStore. Stateless
// apart from the store: each RPC is one store call, so atomicity and fencing
// come from the store. The coordinator, not the worker, chooses the lease.
//
// Error mapping (also documented in epochflow.proto and docs/protocol.md):
// CommitStatus -> OK / NOT_FOUND / FAILED_PRECONDITION / ALREADY_EXISTS;
// std::invalid_argument -> INVALID_ARGUMENT; other std::logic_error (a broken
// invariant) -> INTERNAL; any other exception (for example storage failure,
// outcome unknown) -> UNAVAILABLE.
//
// Thread safety: called concurrently by the gRPC server; relies on the store
// being thread-safe. The store must outlive the service.
class CoordinatorService final : public v1::Coordinator::Service {
 public:
  // Used when SubmitJobRequest.max_attempts is 0.
  static constexpr int kDefaultMaxAttempts = 3;

  CoordinatorService(JobStore& store, Duration lease, LogFn log = {});

  grpc::Status SubmitJob(grpc::ServerContext*, const v1::SubmitJobRequest*,
                         v1::SubmitJobResponse*) override;
  grpc::Status GetJob(grpc::ServerContext*, const v1::GetJobRequest*,
                      v1::GetJobResponse*) override;
  grpc::Status AcquireJob(grpc::ServerContext*, const v1::AcquireJobRequest*,
                          v1::AcquireJobResponse*) override;
  grpc::Status RenewLease(grpc::ServerContext*, const v1::RenewLeaseRequest*,
                          v1::RenewLeaseResponse*) override;
  grpc::Status CompleteJob(grpc::ServerContext*, const v1::CompleteJobRequest*,
                           v1::CompleteJobResponse*) override;
  grpc::Status FailJob(grpc::ServerContext*, const v1::FailJobRequest*,
                       v1::FailJobResponse*) override;
  grpc::Status ApplyLedgerOperation(grpc::ServerContext*, const v1::ApplyLedgerOperationRequest*,
                                    v1::ApplyLedgerOperationResponse*) override;
  grpc::Status BeginExternalOperation(grpc::ServerContext*,
                                      const v1::BeginExternalOperationRequest*,
                                      v1::BeginExternalOperationResponse*) override;
  grpc::Status ConfirmExternalOperation(grpc::ServerContext*,
                                        const v1::ConfirmExternalOperationRequest*,
                                        v1::ConfirmExternalOperationResponse*) override;
  grpc::Status MarkJobUnknown(grpc::ServerContext*, const v1::MarkJobUnknownRequest*,
                              v1::MarkJobUnknownResponse*) override;
  grpc::Status ReconcileJob(grpc::ServerContext*, const v1::ReconcileJobRequest*,
                            v1::ReconcileJobResponse*) override;

 private:
  JobStore& store_;
  Duration lease_;
  LogFn log_;
};

}  // namespace epochflow
