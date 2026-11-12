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

grpc::Status CoordinatorService::ApplyLedgerOperation(
    grpc::ServerContext*, const v1::ApplyLedgerOperationRequest* req,
    v1::ApplyLedgerOperationResponse* resp) {
  return Guard(log_, "ApplyLedgerOperation", [&] {
    const OperationOutcome o = store_.ApplyLedger(req->job_id(), req->generation(),
                                                  req->operation_id(),
                                                  LedgerEntry{req->account(), req->amount_cents()});
    if (o.status == CommitStatus::kAccepted) {
      resp->set_entry_id(o.result);
      resp->set_deduplicated(o.deduplicated);
      if (log_ && o.deduplicated) {
        log_(std::format("deduplicated operation={} job={} generation={} entry={}",
                         req->operation_id(), req->job_id(), req->generation(), o.result));
      }
    } else if (log_) {
      log_(std::format("rejected operation={} job={} generation={} status={}",
                       req->operation_id(), req->job_id(), req->generation(),
                       ToString(o.status)));
    }
    return rpc::ToStatus(o.status);
  });
}

grpc::Status CoordinatorService::BeginExternalOperation(
    grpc::ServerContext*, const v1::BeginExternalOperationRequest* req,
    v1::BeginExternalOperationResponse* resp) {
  return Guard(log_, "BeginExternalOperation", [&] {
    const ExternalBegin b = store_.BeginExternal(req->job_id(), req->generation(),
                                                 req->operation_id(), req->request());
    if (b.status == CommitStatus::kAccepted) {
      resp->set_state(rpc::ToProto(b.state));
      resp->set_confirmation(b.result);
      if (log_ && b.state == ExternalState::kInDoubt) {
        log_(std::format("in doubt operation={} job={} generation={}", req->operation_id(),
                         req->job_id(), req->generation()));
      }
    }
    return rpc::ToStatus(b.status);
  });
}

grpc::Status CoordinatorService::ConfirmExternalOperation(
    grpc::ServerContext*, const v1::ConfirmExternalOperationRequest* req,
    v1::ConfirmExternalOperationResponse*) {
  return Guard(log_, "ConfirmExternalOperation", [&] {
    return rpc::ToStatus(
        store_.ConfirmExternal(req->operation_id(), req->request(), req->confirmation()));
  });
}

grpc::Status CoordinatorService::MarkJobUnknown(grpc::ServerContext*,
                                                const v1::MarkJobUnknownRequest* req,
                                                v1::MarkJobUnknownResponse*) {
  return Guard(log_, "MarkJobUnknown", [&] {
    const CommitStatus s = store_.MarkUnknown(req->job_id(), req->generation(), req->reason());
    if (log_ && s == CommitStatus::kAccepted) {
      log_(std::format("unknown job={} generation={} reason={}", req->job_id(), req->generation(),
                       req->reason()));
    }
    return rpc::ToStatus(s);
  });
}

grpc::Status CoordinatorService::ReconcileJob(grpc::ServerContext*,
                                              const v1::ReconcileJobRequest* req,
                                              v1::ReconcileJobResponse*) {
  return Guard(log_, "ReconcileJob", [&] {
    // FromProto throws std::invalid_argument for unspecified states and the
    // store for non-terminal ones; both become INVALID_ARGUMENT.
    const JobState outcome = rpc::FromProto(req->outcome());
    const std::string& op = req->operation_id();
    const CommitStatus s =
        op.empty() ? store_.Reconcile(req->job_id(), outcome, req->detail())
                   : store_.ReconcileExternal(req->job_id(), op, outcome, req->detail());
    if (log_ && s == CommitStatus::kAccepted) {
      log_(std::format("reconciled job={} outcome={}{}", req->job_id(), ToString(outcome),
                       op.empty() ? "" : " operation=" + op));
    }
    switch (s) {
      case CommitStatus::kStale:
        return grpc::Status{grpc::StatusCode::FAILED_PRECONDITION,
                            op.empty() ? "job is not UNKNOWN, or it has an unresolved external "
                                         "operation (reconcile it with operation_id)"
                                       : "job is not UNKNOWN"};
      case CommitStatus::kNotFound:
        return grpc::Status{grpc::StatusCode::NOT_FOUND,
                            op.empty() ? "job not found" : "job or operation not found"};
      case CommitStatus::kConflict:
        return grpc::Status{grpc::StatusCode::ALREADY_EXISTS,
                            "operation is not this job's external operation, or the decision "
                            "contradicts its recorded confirmation"};
      default:
        return rpc::ToStatus(s);
    }
  });
}

}  // namespace epochflow
