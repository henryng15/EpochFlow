// epochflow-coordinator: serves the coordinator API and reclaims expired leases.
// Milestone 2 keeps job state in memory; it is lost when the process exits.

#include "common/flags.h"
#include "common/log.h"
#include "common/signals.h"
#include "coordinator/coordinator.h"
#include "store/in_memory_job_store.h"

#include <chrono>
#include <exception>
#include <format>
#include <iostream>

namespace {

constexpr const char* kUsage = R"(usage: epochflow-coordinator [flags]

  --listen=HOST:PORT          address to serve on (default 127.0.0.1:50051;
                              port 0 picks a free port)
  --lease-ms=N                lease granted per acquire/renewal (default 10000)
  --reclaim-interval-ms=N     expired-lease recovery period (default 1000)
  --help

Prints "listening on HOST:PORT" to stdout once serving. Stops gracefully on
SIGINT or SIGTERM. Logs events to stderr.
)";

}  // namespace

int main(int argc, char** argv) {
  using namespace epochflow;
  BlockTerminationSignals();
  try {
    const Flags flags(argc, argv, {"listen", "lease-ms", "reclaim-interval-ms", "help"});
    if (flags.Has("help") || !flags.positional().empty()) {
      std::cout << kUsage;
      return flags.Has("help") ? 0 : 2;
    }

    CoordinatorOptions options;
    options.listen_address = flags.GetString("listen", "127.0.0.1:50051");
    options.lease = Duration{flags.GetInt("lease-ms", 10000)};
    options.reclaim_interval = Duration{flags.GetInt("reclaim-interval-ms", 1000)};
    options.log = LogToStderr;

    SystemClock clock;
    InMemoryJobStore store(clock);
    Coordinator coordinator(store, options);

    // The listen host as given, with the port actually bound.
    const auto colon = options.listen_address.rfind(':');
    std::cout << "listening on " << options.listen_address.substr(0, colon) << ':'
              << coordinator.port() << std::endl;
    LogToStderr(std::format("coordinator started lease={}ms reclaim_interval={}ms",
                            options.lease.count(), options.reclaim_interval.count()));

    const int signal = WaitForTerminationSignal();
    LogToStderr(std::format("signal {} received; shutting down", signal));
    coordinator.Shutdown();
    return 0;
  } catch (const std::invalid_argument& e) {
    std::cerr << "error: " << e.what() << "\n\n" << kUsage;
    return 2;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
