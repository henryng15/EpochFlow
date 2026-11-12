// epochflow-worker: acquires jobs from a coordinator and runs the built-in
// demonstration task, which waits --work-ms and returns "processed:<payload>".

#include "client/coordinator_client.h"
#include "common/flags.h"
#include "common/log.h"
#include "common/signals.h"
#include "worker/worker.h"

#include <unistd.h>

#include <chrono>
#include <exception>
#include <format>
#include <iostream>
#include <string>
#include <thread>

namespace {

constexpr const char* kUsage = R"(usage: epochflow-worker [flags]

  --coordinator=HOST:PORT     coordinator address (default 127.0.0.1:50051)
  --worker-id=ID              identity shown in job records (default HOST-PID)
  --work-ms=N                 simulated task duration (default 0)
  --rpc-timeout-ms=N          deadline per RPC (default 5000)
  --idle-backoff-ms=N         wait when no job is available (default 200)
  --help

Stops on SIGINT or SIGTERM after the job in progress finishes and is reported.
Logs events to stderr.
)";

std::string DefaultWorkerId() {
  char host[256] = {};
  if (gethostname(host, sizeof(host) - 1) != 0) return std::format("worker-{}", getpid());
  return std::format("{}-{}", host, getpid());
}

// Simulated work that gives up promptly once the lease is lost.
epochflow::JobHandler DemoTask(std::chrono::milliseconds work) {
  return [work](const epochflow::Assignment& a, std::stop_token lease_lost) -> epochflow::HandlerResult {
    const auto deadline = std::chrono::steady_clock::now() + work;
    while (std::chrono::steady_clock::now() < deadline) {
      if (lease_lost.stop_requested()) return epochflow::JobFailed{"lease lost", true};
      std::this_thread::sleep_for(std::min<std::chrono::milliseconds>(work, std::chrono::milliseconds{10}));
    }
    return epochflow::JobSucceeded{"processed:" + a.payload};
  };
}

}  // namespace

int main(int argc, char** argv) {
  using namespace epochflow;
  BlockTerminationSignals();
  try {
    const Flags flags(argc, argv,
                      {"coordinator", "worker-id", "work-ms", "rpc-timeout-ms", "idle-backoff-ms",
                       "help"});
    if (flags.Has("help") || !flags.positional().empty()) {
      std::cout << kUsage;
      return flags.Has("help") ? 0 : 2;
    }
    const auto work = std::chrono::milliseconds{flags.GetInt("work-ms", 0)};
    if (work.count() < 0) throw std::invalid_argument("--work-ms must be >= 0");

    ClientOptions client_options;
    client_options.rpc_timeout = Duration{flags.GetInt("rpc-timeout-ms", 5000)};
    CoordinatorClient client(flags.GetString("coordinator", "127.0.0.1:50051"), client_options);

    WorkerOptions options;
    options.idle_backoff = Duration{flags.GetInt("idle-backoff-ms", 200)};
    options.log = LogToStderr;
    Worker worker(client, DemoTask(work), flags.GetString("worker-id", DefaultWorkerId()),
                  options);

    std::jthread thread([&](std::stop_token stop) { worker.Run(stop); });
    LogToStderr(std::format("worker={} started", worker.id()));
    const int signal = WaitForTerminationSignal();
    LogToStderr(std::format("worker={} signal {} received; finishing current job", worker.id(),
                            signal));
    thread.request_stop();
    thread.join();
    return 0;
  } catch (const std::invalid_argument& e) {
    std::cerr << "error: " << e.what() << "\n\n" << kUsage;
    return 2;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
