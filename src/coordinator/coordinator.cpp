#include "coordinator/coordinator.h"

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include <chrono>
#include <stdexcept>

namespace epochflow {

Coordinator::Coordinator(JobStore& store, CoordinatorOptions options)
    : service_(store, options.lease, options.log) {
  // Validate before binding so a bad option never leaves a server running.
  if (options.reclaim_interval <= Duration::zero()) {
    throw std::invalid_argument("reclaim interval must be positive");
  }
  grpc::ServerBuilder builder;
  builder.AddListeningPort(options.listen_address, grpc::InsecureServerCredentials(), &port_);
  builder.RegisterService(&service_);
  server_ = builder.BuildAndStart();
  if (!server_ || port_ == 0) {
    throw std::runtime_error("cannot listen on " + options.listen_address);
  }
  reclaimer_ = std::make_unique<Reclaimer>(store, options.reclaim_interval, options.log);
}

Coordinator::~Coordinator() { Shutdown(); }

void Coordinator::Shutdown(Duration grace) {
  if (shut_down_) return;
  shut_down_ = true;
  server_->Shutdown(std::chrono::system_clock::now() + grace);
  reclaimer_->Stop();
}

}  // namespace epochflow
