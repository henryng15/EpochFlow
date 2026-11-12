// epochflow-coordinator: serves the coordinator API and reclaims expired leases.
// With --dsn, job state lives in PostgreSQL and survives restarts; without it,
// state is kept in memory and lost when the process exits.

#include "common/flags.h"
#include "common/log.h"
#include "common/signals.h"
#include "coordinator/coordinator.h"
#include "store/in_memory_job_store.h"
#include "store/migrations.h"
#include "store/pg_job_store.h"

#include <chrono>
#include <exception>
#include <format>
#include <iostream>
#include <memory>

namespace {

constexpr const char* kUsage = R"(usage: epochflow-coordinator [flags]

  --listen=HOST:PORT          address to serve on (default 127.0.0.1:50051;
                              port 0 picks a free port)
  --lease-ms=N                lease granted per acquire/renewal (default 10000)
  --reclaim-interval-ms=N     expired-lease recovery period (default 1000)
  --dsn=CONNINFO              PostgreSQL connection string; applies pending
                              migrations at startup. Without it, job state is
                              in memory only and lost on exit.
  --db-pool-size=N            PostgreSQL connections (default 4)
  --help

Prints "listening on HOST:PORT" to stdout once serving. Stops gracefully on
SIGINT or SIGTERM. Logs events to stderr.
)";

}  // namespace

int main(int argc, char** argv) {
  using namespace epochflow;
  BlockTerminationSignals();
  try {
    const Flags flags(argc, argv,
                      {"listen", "lease-ms", "reclaim-interval-ms", "dsn", "db-pool-size", "help"});
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
    std::unique_ptr<JobStore> store;
    std::string store_name;
    if (flags.Has("dsn")) {
      const std::string dsn = flags.GetString("dsn", "");
      MigrateDatabase(dsn);
      PgStoreOptions pg_options;
      pg_options.pool_size = static_cast<int>(flags.GetInt("db-pool-size", 4));
      store = std::make_unique<PgJobStore>(dsn, pg_options);
      store_name = "postgres";
    } else {
      store = std::make_unique<InMemoryJobStore>(clock);
      store_name = "memory";
      LogToStderr("warning: no --dsn; job state is in memory and will be lost on exit");
    }
    Coordinator coordinator(*store, options);

    LogToStderr(std::format("coordinator started store={} lease={}ms reclaim_interval={}ms",
                            store_name, options.lease.count(), options.reclaim_interval.count()));
    // Printed last: scripts treat this line as "ready", so everything above
    // (including the log line) has already happened when they see it.
    const auto colon = options.listen_address.rfind(':');
    std::cout << "listening on " << options.listen_address.substr(0, colon) << ':'
              << coordinator.port() << std::endl;

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
