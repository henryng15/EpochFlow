#include "coordinator/coordinator_service.h"

#include "rpc/convert.h"

#include <exception>
#include <format>
#include <stdexcept>
#include <utility>

namespace epochflow {
namespace {

// Runs one RPC body and converts exceptions to the documented status codes.
template <typename Body>
grpc::Status Guard(const LogFn& log, const char* rpc, Body&& body) {
  try {
    return body();
  } catch (const std::invalid_argument& e) {
    return {grpc::StatusCode::INVALID_ARGUMENT, e.what()};
  } catch (const std::logic_error& e) {
    if (log) log(std::format("{}: invariant violation: {}", rpc, e.what()));
    return {grpc::StatusCode::INTERNAL, e.what()};
  } catch (const std::exception& e) {
    if (log) log(std::format("{}: store error: {}", rpc, e.what()));
    return {grpc::StatusCode::UNAVAILABLE, e.what()};
  }
}

}  // namespace

CoordinatorService::CoordinatorService(JobStore& store, Duration lease, LogFn log)
    : store_(store), lease_(lease), log_(std::move(log)) {
  if (lease_ <= Duration::zero()) throw std::invalid_argument("lease must be positive");
}

grpc::Status CoordinatorService::SubmitJob(grpc::ServerContext*, const v1::SubmitJobRequest* req,
                                           v1::SubmitJobResponse* resp) {
  return Guard(log_, "SubmitJob", [&] {
    if (req->max_attempts() < 0) throw std::invalid_argument("max_attempts must be >= 0");
    const int max_attempts = req->max_attempts() == 0 ? kDefaultMaxAttempts : req->max_attempts();
    resp->set_job_id(store_.Submit(JobSpec{req->payload(), max_attempts}));
    return grpc::Status::OK;
  });
}

grpc::Status CoordinatorService::GetJob(grpc::ServerContext*, const v1::GetJobRequest* req,
                                        v1::GetJobResponse* resp) {
  return Guard(log_, "GetJob", [&] {
    const auto job = store_.Get(req->job_id());
    if (!job) return rpc::ToStatus(CommitStatus::kNotFound);
    rpc::ToProto(*job, resp->mutable_job());
    return grpc::Status::OK;
  });
}

grpc::Status CoordinatorService::AcquireJob(grpc::ServerContext*, const v1::AcquireJobRequest* req,
                                            v1::AcquireJobResponse* resp) {
  return Guard(log_, "AcquireJob", [&] {
    if (req->worker_id().empty()) throw std::invalid_argument("worker_id is required");
    const auto assignment = store_.Acquire(req->worker_id(), lease_);
    if (assignment) {
      rpc::ToProto(*assignment, resp->mutable_assignment());
      if (log_) {
        log_(std::format("assigned job={} generation={} worker={}", assignment->job_id,
                         assignment->generation, req->worker_id()));
      }
    }
    return grpc::Status::OK;
  });
}

grpc::Status CoordinatorService::RenewLease(grpc::ServerContext*, const v1::RenewLeaseRequest* req,
                                            v1::RenewLeaseResponse* resp) {
  return Guard(log_, "RenewLease", [&] {
    const RenewResult r = store_.Renew(req->job_id(), req->generation(), lease_);
    if (r.status == CommitStatus::kAccepted) resp->set_lease_until_unix_ms(rpc::ToUnixMs(r.lease_until));
    return rpc::ToStatus(r.status);
  });
}

grpc::Status CoordinatorService::CompleteJob(grpc::ServerContext*,
                                             const v1::CompleteJobRequest* req,
                                             v1::CompleteJobResponse*) {
  return Guard(log_, "CompleteJob", [&] {
    const CommitStatus s = store_.Complete(req->job_id(), req->generation(), req->result());
    if (log_ && s != CommitStatus::kAccepted) {
      log_(std::format("rejected completion job={} generation={} status={}", req->job_id(),
                       req->generation(), ToString(s)));
    }
    return rpc::ToStatus(s);
  });
}

grpc::Status CoordinatorService::FailJob(grpc::ServerContext*, const v1::FailJobRequest* req,
                                         v1::FailJobResponse*) {
  return Guard(log_, "FailJob", [&] {
    return rpc::ToStatus(
        store_.Fail(req->job_id(), req->generation(), req->reason(), req->retryable()));
  });
}

}  // namespace epochflow
