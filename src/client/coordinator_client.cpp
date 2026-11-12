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

CommitStatus CoordinatorClient::MarkUnknown(JobId id, Generation generation,
                                            const std::string& reason) {
  v1::MarkJobUnknownRequest req;
  req.set_job_id(id);
  req.set_generation(generation);
  req.set_reason(reason);
  v1::MarkJobUnknownResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  return rpc::ToCommitStatus(stub_->MarkJobUnknown(&ctx, req, &resp));
}

OperationOutcome CoordinatorClient::ApplyLedger(JobId id, Generation generation,
                                                const std::string& operation_id,
                                                const LedgerEntry& entry) {
  v1::ApplyLedgerOperationRequest req;
  req.set_job_id(id);
  req.set_generation(generation);
  req.set_operation_id(operation_id);
  req.set_account(entry.account);
  req.set_amount_cents(entry.amount_cents);
  v1::ApplyLedgerOperationResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  const CommitStatus status = rpc::ToCommitStatus(stub_->ApplyLedgerOperation(&ctx, req, &resp));
  return {status, resp.entry_id(), resp.deduplicated()};
}

ExternalBegin CoordinatorClient::BeginExternal(JobId id, Generation generation,
                                               const std::string& operation_id,
                                               const std::string& request) {
  v1::BeginExternalOperationRequest req;
  req.set_job_id(id);
  req.set_generation(generation);
  req.set_operation_id(operation_id);
  req.set_request(request);
  v1::BeginExternalOperationResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  const CommitStatus status = rpc::ToCommitStatus(stub_->BeginExternalOperation(&ctx, req, &resp));
  if (status != CommitStatus::kAccepted) return {status, ExternalState::kStart, {}};
  return {status, rpc::FromProto(resp.state()), resp.confirmation()};
}

CommitStatus CoordinatorClient::ConfirmExternal(const std::string& operation_id,
                                                const std::string& request,
                                                const std::string& confirmation) {
  v1::ConfirmExternalOperationRequest req;
  req.set_operation_id(operation_id);
  req.set_request(request);
  req.set_confirmation(confirmation);
  v1::ConfirmExternalOperationResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  return rpc::ToCommitStatus(stub_->ConfirmExternalOperation(&ctx, req, &resp));
}

CommitStatus CoordinatorClient::Reconcile(JobId id, JobState outcome, const std::string& detail) {
  v1::ReconcileJobRequest req;
  req.set_job_id(id);
  req.set_outcome(rpc::ToProto(outcome));
  req.set_detail(detail);
  v1::ReconcileJobResponse resp;
  grpc::ClientContext ctx;
  Prepare(ctx);
  return rpc::ToCommitStatus(stub_->ReconcileJob(&ctx, req, &resp));
}

}  // namespace epochflow
