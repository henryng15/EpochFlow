#include "client/coordinator_client.h"

#include "rpc/convert.h"
#include "rpc/rpc_error.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

namespace epochflow {

CoordinatorClient::CoordinatorClient(const std::string& target, ClientOptions options)
    : stub_(v1::Coordinator::NewStub(
          grpc::CreateChannel(target, grpc::InsecureChannelCredentials()))),
      options_(options) {}

void CoordinatorClient::Prepare(grpc::ClientContext& context) const {
  context.set_deadline(std::chrono::system_clock::now() + options_.rpc_timeout);
}

JobId CoordinatorClient::Submit(const JobSpec& spec) {
  v1::SubmitJobRequest req;
  req.set_payload(spec.payload);
  req.set_max_attempts(spec.max_attempts);
  v1::SubmitJobResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  const grpc::Status status = stub_->SubmitJob(&ctx, req, &resp);
  if (!status.ok()) throw RpcError(status);
  return resp.job_id();
}

std::optional<JobRecord> CoordinatorClient::Get(JobId id) {
  v1::GetJobRequest req;
  req.set_job_id(id);
  v1::GetJobResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  const grpc::Status status = stub_->GetJob(&ctx, req, &resp);
  if (status.error_code() == grpc::StatusCode::NOT_FOUND) return std::nullopt;
  if (!status.ok()) throw RpcError(status);
  return rpc::FromProto(resp.job());
}

std::optional<Assignment> CoordinatorClient::Acquire(const WorkerId& worker) {
  v1::AcquireJobRequest req;
  req.set_worker_id(worker);
  v1::AcquireJobResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  const grpc::Status status = stub_->AcquireJob(&ctx, req, &resp);
  if (!status.ok()) throw RpcError(status);
  if (!resp.has_assignment()) return std::nullopt;
  return rpc::FromProto(resp.assignment());
}

CommitStatus CoordinatorClient::Renew(JobId id, Generation generation) {
  v1::RenewLeaseRequest req;
  req.set_job_id(id);
  req.set_generation(generation);
  v1::RenewLeaseResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  return rpc::ToCommitStatus(stub_->RenewLease(&ctx, req, &resp));
}

CommitStatus CoordinatorClient::Complete(JobId id, Generation generation,
                                         const std::string& result) {
  v1::CompleteJobRequest req;
  req.set_job_id(id);
  req.set_generation(generation);
  req.set_result(result);
  v1::CompleteJobResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  return rpc::ToCommitStatus(stub_->CompleteJob(&ctx, req, &resp));
}

CommitStatus CoordinatorClient::Fail(JobId id, Generation generation, const std::string& reason,
                                     bool retryable) {
  v1::FailJobRequest req;
  req.set_job_id(id);
  req.set_generation(generation);
  req.set_reason(reason);
  req.set_retryable(retryable);
  v1::FailJobResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  return rpc::ToCommitStatus(stub_->FailJob(&ctx, req, &resp));
}

}  // namespace epochflow
