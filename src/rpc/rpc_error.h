#pragma once

#include <grpcpp/support/status.h>

#include <stdexcept>
#include <string>

namespace epochflow {

// A gRPC call that did not produce a JobStore-level answer: transport failure,
// timeout, invalid argument, or server error. For UNAVAILABLE,
// DEADLINE_EXCEEDED, and INTERNAL the request may or may not have been applied.
class RpcError : public std::runtime_error {
 public:
  explicit RpcError(const grpc::Status& status)
      : std::runtime_error("rpc failed (" + std::to_string(status.error_code()) +
                           "): " + status.error_message()),
        code_(status.error_code()) {}

  grpc::StatusCode code() const { return code_; }

 private:
  grpc::StatusCode code_;
};

}  // namespace epochflow
