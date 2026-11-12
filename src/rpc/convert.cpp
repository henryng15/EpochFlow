#include "rpc/convert.h"

#include "rpc/rpc_error.h"

#include <chrono>
#include <stdexcept>

namespace epochflow::rpc {

v1::JobState ToProto(JobState state) {
  switch (state) {
    case JobState::kPending:
      return v1::JOB_STATE_PENDING;
    case JobState::kRunning:
      return v1::JOB_STATE_RUNNING;
    case JobState::kCompleted:
      return v1::JOB_STATE_COMPLETED;
    case JobState::kFailed:
      return v1::JOB_STATE_FAILED;
    case JobState::kUnknown:
      return v1::JOB_STATE_UNKNOWN;
  }
  return v1::JOB_STATE_UNSPECIFIED;
}

JobState FromProto(v1::JobState state) {
  switch (state) {
    case v1::JOB_STATE_PENDING:
      return JobState::kPending;
    case v1::JOB_STATE_RUNNING:
      return JobState::kRunning;
    case v1::JOB_STATE_COMPLETED:
      return JobState::kCompleted;
    case v1::JOB_STATE_FAILED:
      return JobState::kFailed;
    case v1::JOB_STATE_UNKNOWN:
      return JobState::kUnknown;
    default:
      throw std::invalid_argument("unsupported job state on the wire");
  }
}

std::int64_t ToUnixMs(TimePoint t) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

TimePoint FromUnixMs(std::int64_t ms) { return TimePoint{std::chrono::milliseconds{ms}}; }

void ToProto(const JobRecord& job, v1::Job* out) {
  out->set_job_id(job.id);
  out->set_state(ToProto(job.state));
  out->set_generation(job.generation);
  if (job.worker_id) out->set_worker_id(*job.worker_id);
  if (job.lease_until) out->set_lease_until_unix_ms(ToUnixMs(*job.lease_until));
  out->set_attempts(job.attempts);
  out->set_max_attempts(job.max_attempts);
  out->set_payload(job.payload);
  if (job.result) out->set_result(*job.result);
  if (job.error) out->set_error(*job.error);
}

JobRecord FromProto(const v1::Job& job) {
  JobRecord r;
  r.id = job.job_id();
  r.state = FromProto(job.state());
  r.generation = job.generation();
  if (!job.worker_id().empty()) r.worker_id = job.worker_id();
  if (job.lease_until_unix_ms() != 0) r.lease_until = FromUnixMs(job.lease_until_unix_ms());
  r.attempts = job.attempts();
  r.max_attempts = job.max_attempts();
  r.payload = job.payload();
  if (job.has_result()) r.result = job.result();
  if (job.has_error()) r.error = job.error();
  return r;
}

void ToProto(const Assignment& a, v1::Assignment* out) {
  out->set_job_id(a.job_id);
  out->set_generation(a.generation);
  out->set_payload(a.payload);
  out->set_lease_until_unix_ms(ToUnixMs(a.lease_until));
  out->set_lease_ms(a.lease.count());
  out->set_attempt(a.attempt);
}

Assignment FromProto(const v1::Assignment& a) {
  return Assignment{a.job_id(), a.generation(), a.payload(), FromUnixMs(a.lease_until_unix_ms()),
                    Duration{a.lease_ms()}, a.attempt()};
}

grpc::Status ToStatus(CommitStatus status) {
  switch (status) {
    case CommitStatus::kAccepted:
      return grpc::Status::OK;
    case CommitStatus::kNotFound:
      return {grpc::StatusCode::NOT_FOUND, "job not found"};
    case CommitStatus::kStale:
      return {grpc::StatusCode::FAILED_PRECONDITION, "assignment is not current"};
    case CommitStatus::kConflict:
      return {grpc::StatusCode::ALREADY_EXISTS, "job already finished with different content"};
  }
  return {grpc::StatusCode::INTERNAL, "unknown commit status"};
}

CommitStatus ToCommitStatus(const grpc::Status& status) {
  switch (status.error_code()) {
    case grpc::StatusCode::OK:
      return CommitStatus::kAccepted;
    case grpc::StatusCode::NOT_FOUND:
      return CommitStatus::kNotFound;
    case grpc::StatusCode::FAILED_PRECONDITION:
      return CommitStatus::kStale;
    case grpc::StatusCode::ALREADY_EXISTS:
      return CommitStatus::kConflict;
    default:
      throw RpcError(status);
  }
}

}  // namespace epochflow::rpc
