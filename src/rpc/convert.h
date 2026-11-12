#pragma once

// Mapping between domain types and the epochflow.v1 protocol, shared by the
// coordinator service and the client so both sides agree on the wire format.

#include "core/clock.h"
#include "core/job.h"
#include "core/job_state.h"
#include "epochflow/v1/epochflow.pb.h"

#include <grpcpp/support/status.h>

#include <cstdint>

namespace epochflow::rpc {

v1::JobState ToProto(JobState state);
// Throws std::invalid_argument for JOB_STATE_UNSPECIFIED or unknown values.
JobState FromProto(v1::JobState state);

std::int64_t ToUnixMs(TimePoint t);
TimePoint FromUnixMs(std::int64_t ms);

void ToProto(const JobRecord& job, v1::Job* out);
JobRecord FromProto(const v1::Job& job);

void ToProto(const Assignment& a, v1::Assignment* out);
Assignment FromProto(const v1::Assignment& a);

// Server side: the gRPC status for a store outcome (see the error model in
// epochflow.proto).
grpc::Status ToStatus(CommitStatus status);

// Client side: the store outcome carried by a gRPC status. Throws RpcError for
// codes that carry no store outcome (transport errors, invalid arguments).
CommitStatus ToCommitStatus(const grpc::Status& status);

}  // namespace epochflow::rpc
