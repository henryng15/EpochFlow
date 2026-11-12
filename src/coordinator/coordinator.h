#pragma once

#include "common/log.h"
#include "coordinator/coordinator_service.h"
#include "coordinator/reclaimer.h"
#include "core/clock.h"
#include "core/job_store.h"

#include <grpcpp/server.h>

#include <chrono>
#include <memory>
#include <string>

namespace epochflow {

struct CoordinatorOptions {
  // host:port to listen on; port 0 picks a free port (see Coordinator::port).
  std::string listen_address = "127.0.0.1:50051";
  // Lease granted on every acquire and renewal.
  Duration lease = std::chrono::seconds{10};
  // Period of the expired-lease recovery loop.
  Duration reclaim_interval = std::chrono::seconds{1};
  LogFn log;
};

// The coordinator process core: a gRPC server for CoordinatorService plus a
// Reclaimer, both over one JobStore. Starts serving in the constructor.
//
// Insecure (plaintext, unauthenticated) transport: bind to loopback or a
// trusted network only.
class Coordinator {
 public:
  // Throws std::runtime_error if the address cannot be bound, and
  // std::invalid_argument for non-positive durations. The store must outlive
  // the coordinator.
  Coordinator(JobStore& store, CoordinatorOptions options);
  ~Coordinator();

  Coordinator(const Coordinator&) = delete;
  Coordinator& operator=(const Coordinator&) = delete;

  // Port actually bound.
  int port() const { return port_; }

  // Stops accepting RPCs, gives in-flight RPCs up to `grace` to finish, then
  // stops the reclaimer. Idempotent. Must not race with destruction.
  void Shutdown(Duration grace = std::chrono::seconds{5});

 private:
  CoordinatorService service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<Reclaimer> reclaimer_;
  int port_ = 0;
  bool shut_down_ = false;
};

}  // namespace epochflow
